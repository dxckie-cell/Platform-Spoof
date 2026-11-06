import { getModules } from '@valenced/modules/finders'
import { withProps } from '@valenced/modules/finders/filters'
import { after, before } from '@valenced/patcher'
import SettingsComponent from './settings'
import type { JsonStorage } from '@valenced/json-storage'

const DEBUG: boolean = false

export const Platforms = ['off', 'desktop', 'web', 'meta', 'console'] as const
export type Platform = (typeof Platforms)[number]
export const DefaultPlatform: Platform = 'off'

export interface Settings {
	platform: Platform
	/** Last gateway close seen while spoofing, shown in settings for debugging. */
	lastClose?: string
}

/** Gateway IDENTIFY properties that make Discord show each client icon. */
const SpoofedProperties: Record<
	Exclude<Platform, 'off'>,
	{ browser: string }
> = {
	desktop: { browser: 'Discord Client' },
	web: { browser: 'Chrome' },
	meta: { browser: 'Discord VR' },
	console: { browser: 'Discord Embedded' },
}

const HeartbeatOp = 1
const IdentifyOp = 2
const ResumeOp = 6
/** How long after a forced reconnect to hold back messages sent before login. */
const GuardWindowMs = 20_000
/** Gateway close code for a rejected token; Discord's client logs you out on it. */
const AuthFailedCode = 4004
/** Reconnectable close code the client answers with a fresh connection. */
const ReconnectCode = 4000
const WebSocketConnecting = 0
const WebSocketOpen = 1
/** The gateway rate-limits IDENTIFY, so reconnects closer than this are deferred. */
const MinReconnectIntervalMs = 3000

interface Transport {
	readyState: number
	onclose?: unknown
	send(data: unknown): void
	close(): void
}

interface CloseEventLike {
	code?: number
	reason?: string
}

interface GatewaySocket {
	webSocket?: Transport
	sessionId: string | null
	seq: number
	send(op: number, data: unknown, flag?: unknown): void
	handleIdentify(...args: unknown[]): unknown
	connect(): void
	close(): void
}

interface SocketModule {
	getSocket(): GatewaySocket | null
	isConnected(): boolean
}

interface IdentifyPayload {
	properties?: Record<string, unknown>
}

type Timer = ReturnType<typeof setTimeout>

let storage: JsonStorage<Settings> | undefined
let reportError: (error: unknown) => void
let socketModule: SocketModule | undefined
let patchedSocket: GatewaySocket | undefined
let unpatchSocket: (() => void) | undefined
let retryPending = false
let lastIdentifyAt = 0
/** Sockets we already forced a fresh IDENTIFY on, so boot polling never repeats it. */
let forcedSockets = new WeakSet<GatewaySocket>()
let transportErrorReported = false
let authRescueUsed = false
let guardUntil = 0
/** Messages held back until the session is ready, per connection. */
const heldFrames = new WeakMap<Transport, string[]>()
const transportPatches = new Map<Transport, () => void>()
const timers = new Set<Timer>()
const identifyListeners = new Set<() => void>()

const getSocket = () => socketModule?.getSocket() ?? null
const getPlatform = (): Platform => storage?.cache?.platform ?? DefaultPlatform

/** True once the gateway has a live session again after a forced reconnect. */
export function isSessionReady() {
	const socket = getSocket()
	return socket?.webSocket?.readyState === WebSocketOpen && !!socket.sessionId
}

export const hasIdentified = () => lastIdentifyAt !== 0

/** Calls `listener` whenever a reconnect is forced. */
export function onIdentify(listener: () => void) {
	identifyListeners.add(listener)
	return () => {
		identifyListeners.delete(listener)
	}
}

function later(fn: () => void, ms: number) {
	const timer = setTimeout(() => {
		timers.delete(timer)
		fn()
	}, ms)
	timers.add(timer)
	return timer
}

/** Runs `tick` every `everyMs` until it returns true or `maxTries` is reached. */
function poll(everyMs: number, maxTries: number, tick: () => boolean | void) {
	let tries = 0
	const next = () =>
		later(() => {
			if (tick() || ++tries >= maxTries) return
			next()
		}, everyMs)
	next()
}

let spoofApplied = false

function applySpoof(payload: IdentifyPayload | undefined) {
	const platform = getPlatform()
	if (platform === 'off' || !payload?.properties) return false
	Object.assign(payload.properties, SpoofedProperties[platform])
	spoofApplied = true
	return true
}

const closeLog: string[] = []
/** Ops sent on each gateway connection, to see what led to a rejected close. */
const sentOps = new WeakMap<Transport, { t0: number; ops: string[] }>()

function trackSend(transport: Transport, op: unknown) {
	let entry = sentOps.get(transport)
	if (!entry) sentOps.set(transport, (entry = { t0: Date.now(), ops: [] }))
	entry.ops.push(`${op}@${((Date.now() - entry.t0) / 1000).toFixed(1)}`)
	if (entry.ops.length > 8) entry.ops.shift()
}

const describeOps = (transport: Transport) =>
	`sent[${sentOps.get(transport)?.ops.join(',') ?? ''}]`

function recordClose(note: string, platform: Platform = getPlatform()) {
	closeLog.push(note)
	if (closeLog.length > 3) closeLog.shift()
	try {
		storage?.set({ platform, lastClose: closeLog.join(' | ') })
	} catch (e) {
		reportError(e)
	}
}

/**
 * Watches gateway closes while spoofing. If Discord rejects the spoofed login
 * with an auth failure, the client would log the account out. Turn the spoof
 * off and report a reconnectable close instead, once per forced identify.
 */
function wrapClose(transport: Transport) {
	const original = transport.onclose
	if (typeof original !== 'function') return () => {}

	const wrapped = function (this: unknown, event: CloseEventLike) {
		let forwarded = event
		try {
			if (getPlatform() !== 'off') {
				const code = event?.code
				const when = new Date().toISOString()
				if (code === AuthFailedCode && !authRescueUsed) {
					authRescueUsed = true
					const note = `${code} ${event?.reason ?? ''} ${describeOps(transport)} at ${when}: spoof turned off to avoid logout`
					recordClose(note, DefaultPlatform)
					forwarded = { ...event, code: ReconnectCode }
				} else if (code !== undefined && code !== 1000 && code !== 1005) {
					recordClose(
						`${code} ${event?.reason ?? ''} ${describeOps(transport)} at ${when}`,
					)
				}
			}
		} catch (e) {
			reportError(e)
		}
		return (original as (event: CloseEventLike) => unknown).call(this, forwarded)
	}

	transport.onclose = wrapped
	return () => {
		if (transport.onclose === wrapped) transport.onclose = original
	}
}

function holdFrame(
	transport: Transport,
	socket: GatewaySocket,
	data: string,
	send: (...a: unknown[]) => unknown,
) {
	const queue = heldFrames.get(transport)
	if (queue) {
		queue.push(data)
		return
	}
	heldFrames.set(transport, [data])

	const tick = (n: number): void => {
		const frames = heldFrames.get(transport)
		if (!frames) return
		if (transport.readyState !== WebSocketOpen || n > 150) {
			heldFrames.delete(transport)
			return
		}
		if (!socket.sessionId) return later(() => tick(n + 1), 100)
		heldFrames.delete(transport)
		for (const frame of frames) {
			try {
				send.call(transport, frame)
			} catch (e) {
				reportError(e)
			}
		}
	}
	later(() => tick(0), 100)
}

function patchTransport(socket: GatewaySocket) {
	const transport = socket.webSocket
	if (!transport || typeof transport.send !== 'function') return
	if (transportPatches.has(transport)) return

	const unwrapClose = wrapClose(transport)
	const originalSend = transport.send
	const wrappedSend = function (this: Transport, ...args: unknown[]) {
			const call = (...next: unknown[]) =>
				(originalSend as (...a: unknown[]) => unknown).apply(this ?? transport, next)
			const [data] = args
			if (typeof data !== 'string') {
				trackSend(transport, 'bin')
				return call(...args)
			}

			let out = data
			try {
				const frame = JSON.parse(data)
				const op = frame?.op

				// After a forced reconnect the app still thinks it is ready, so it
				// sends its usual messages (voice state, subscriptions...) right after
				// IDENTIFY, while Discord is still building the session. Discord
				// closes that with 4003 Not authenticated, which ends in 4004 and a
				// logout. Hold those back until READY, then send them in order.
				if (
					Date.now() < guardUntil &&
					!socket.sessionId &&
					op !== HeartbeatOp &&
					op !== IdentifyOp &&
					op !== ResumeOp
				) {
					trackSend(transport, `x${op}`)
					holdFrame(transport, socket, data, originalSend)
					return
				}

				trackSend(transport, op)
				if (op === IdentifyOp && applySpoof(frame.d)) out = JSON.stringify(frame)
			} catch (e) {
				if (!transportErrorReported) {
					transportErrorReported = true
					reportError(e)
				}
			}
			return call(out, ...args.slice(1))
	}
	transport.send = wrappedSend
	const unpatchSend = () => {
		if (transport.send === wrappedSend) transport.send = originalSend
	}
	transportPatches.set(transport, () => {
		unpatchSend()
		unwrapClose()
	})
}

function unpatchTransports() {
	for (const unpatch of transportPatches.values()) unpatch()
	transportPatches.clear()
}

function patchSocket(socket: GatewaySocket) {
	unpatchSocket?.()
	unpatchTransports()

	const unpatches = [
		before(socket, 'send', args => {
			if (args[0] === IdentifyOp) applySpoof(args[1] as IdentifyPayload)
			return args
		}),
	]
	// Every reconnect opens a new transport that needs patching again.
	if (typeof socket.handleIdentify === 'function')
		unpatches.push(
			after(socket, 'handleIdentify', result => {
				patchTransport(socket)
				return result
			}),
		)

	patchedSocket = socket
	unpatchSocket = () => {
		for (const unpatch of unpatches) unpatch()
	}
	patchTransport(socket)
}

/** Moves the patches over if Discord replaced the gateway socket under us. */
function retargetToLiveSocket(stale: GatewaySocket) {
	const live = getSocket()
	if (!live || live === stale) return false

	if (DEBUG) console.log('[PlatformSpoof] socket instance changed, retargeting')
	patchSocket(live)
	if (!live.webSocket) watchForNewTransport(live)
	return true
}

function watchForNewTransport(socket: GatewaySocket, previous?: Transport) {
	poll(200, 40, () => {
		if (retargetToLiveSocket(socket)) return true
		const current = socket.webSocket
		if (!current) return false
		patchTransport(socket)
		return current !== previous
	})
}

/** Waits for the old transport to go away, then reconnects if Discord doesn't. */
function awaitReconnect(socket: GatewaySocket, previous?: Transport) {
	const attempt = (delayMs: number, tries: number): void => {
		later(() => {
			if (retargetToLiveSocket(socket)) return

			const fresh = socket.webSocket
			if (
				fresh &&
				fresh !== previous &&
				(fresh.readyState === WebSocketConnecting ||
					fresh.readyState === WebSocketOpen)
			) {
				watchForNewTransport(socket, previous)
				return
			}
			if (tries < 12) return attempt(250, tries + 1)

			try {
				socket.connect()
			} catch (e) {
				reportError(e)
			}
			watchForNewTransport(socket, previous)
		}, delayMs)
	}

	// Always close through Discord's own socket so its heartbeat timers stop.
	// Closing only the raw transport left a stale heartbeat that was sent on the
	// next connection before it logged in (gateway close 4003 Not authenticated),
	// which then ended in 4004 and a logout.
	socket.close()
	attempt(500, 0)
}

/** Drops the current session so the gateway sends a fresh IDENTIFY. */
export function forceIdentify() {
	const waitMs = lastIdentifyAt + MinReconnectIntervalMs - Date.now()
	if (waitMs > 0) {
		if (!retryPending) {
			retryPending = true
			later(() => {
				retryPending = false
				forceIdentify()
			}, waitMs + 50)
		}
		return
	}

	const socket = getSocket()
	if (!socket) return

	try {
		if (socket !== patchedSocket) patchSocket(socket)
		else patchTransport(socket)

		lastIdentifyAt = Date.now()
		authRescueUsed = false
		guardUntil = Date.now() + GuardWindowMs
		forcedSockets.add(socket)
		for (const listener of identifyListeners) listener()

		socket.sessionId = null
		socket.seq = 0
		awaitReconnect(socket, socket.webSocket)
	} catch (e) {
		reportError(e)
	}
}

/**
 * At launch, wait for the first login to finish. Only force a reconnect if that
 * login went out unspoofed; forcing it mid-connect got the account logged out.
 */
function forceAfterReady(socket: GatewaySocket) {
	if (forcedSockets.has(socket)) return
	forcedSockets.add(socket)
	poll(100, 200, () => {
		if (!socket.sessionId) return false
		if (!spoofApplied && getPlatform() !== 'off') forceIdentify()
		return true
	})
}

function attachToGateway() {
	const attach = () => {
		const socket = getSocket()
		if (!socket) return false
		patchSocket(socket)
		if (getPlatform() !== 'off') forceAfterReady(socket)
		return true
	}
	if (!attach()) poll(50, 150, attach)

	// Discord can swap the socket again while it is still booting.
	// Starting from null makes the first tick re-force the identify, which
	// catches cold starts where the gateway was already connected.
	let lastSeen: GatewaySocket | null = null
	poll(500, 30, () => {
		const live = getSocket()
		if (!live || live === lastSeen) return
		lastSeen = live
		if (live !== patchedSocket) patchSocket(live)
		// attach() already forced this socket; forcing again would open a second
		// fresh session a few seconds after the first (suspected cause of logouts).
		if (getPlatform() !== 'off') forceAfterReady(live)
	})
}

function teardown() {
	for (const timer of timers) clearTimeout(timer)
	timers.clear()
	retryPending = false
	lastIdentifyAt = 0
	forcedSockets = new WeakSet()
	spoofApplied = false
	closeLog.length = 0
	guardUntil = 0
	unpatchSocket?.()
	unpatchTransports()
	unpatchSocket = patchedSocket = socketModule = storage = undefined
}

export default plugin<{ jsonStorage: Settings }>({
	jsonStorage: {
		load: true,
		default: { platform: DefaultPlatform },
	},
	async start({ cleanup, jsonStorage, plugin }) {
		await jsonStorage.get()
		storage = jsonStorage
		reportError = e => plugin.reportError(e)

		cleanup(
			teardown,
			getModules(withProps<SocketModule>('getSocket', 'isConnected'), mod => {
				socketModule = mod
				attachToGateway()
			}),
		)
	},
	SettingsComponent,
})
