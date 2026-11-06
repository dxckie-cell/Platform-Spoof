# Platform Spoof

A [Valenced](https://github.com/Valenced) plugin that spoofs your Discord gateway platform so others see a different client icon on your profile (Desktop, Web, Meta Quest / VR, Console).

Originally made by [k1ng0p](https://github.com/k1ng0p). This fork avoids a double forced login at startup and, if Discord rejects the spoofed login (close code 4004), turns the spoof off and reconnects instead of logging you out. The settings page shows the last gateway close code.

> [!WARNING]
> Spoofing the gateway IDENTIFY payload is against Discord's Terms of Service. Use at your own risk.

## Installation

Add this repository through **Settings → General → Plugins → Add Repository**.

```text
https://dxckie-cell.github.io/Platform-Spoof/
```
