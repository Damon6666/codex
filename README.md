> [!NOTE]
> **This is the `v0demo` branch — Pulsing 多 Codex 协作演示。**
> 在 Codex CLI 上嵌入 Pulsing 分布式 actor mesh：10 个 codex 实例协作完成
> "派单 → 写码 → 8 店审查 → 打回修复 → 机械验收 → 交付"全流程，全程可观测（实时时序图 / 明文账本 / 马拉松验收）。
>
> ## 拉下来就能跑（Linux / WSL，已实测）
>
> 前置只有两个：**Rust 工具链** + **模型接入**（`codex login`，或在 `~/.codex/config.toml` 配任意 OpenAI 兼容端点）。
>
> ```bash
> # 1. 克隆（--recursive 会自动拉取内核 submodule）
> git clone --recursive -b v0demo https://github.com/Damon6666/codex.git codex-demo
> cd codex-demo/pulsing-demo
>
> # 2. 构建（一次性，约 10-30 分钟；产物 demo.sh 会自动找到）
> cd ../codex-rs && cargo build --release -p codex-cli && cd ../pulsing-demo
>
> # 3. 跑单机 10-codex demo（无需第二台机器/隧道/代理）
> bash scripts/demo.sh single10 mydemo
>
> # 另开一个窗口：实时全场日志+时序图（浏览器 http://localhost:8765）
> bash scripts/demo.sh watch mydemo
> ```
>
> 正常约 5 分钟走完全流程并自动退出（事件账本落在 `demo/<tag>/`）。
> 最小验证可先用 `bash scripts/demo.sh single mydemo`（3-codex）。
> **自建模型网关的用户必读**：模型名保持 `gpt-5.6-luna`、在网关侧映射到实际后端（陌生模型名会降级 fork 的工具调用）。
> 详细说明（分层/机制/跨机进阶/常见问题）：**[pulsing-demo/README.md](pulsing-demo/README.md)**
>
> 以下为上游 Codex CLI 原版说明。

<p align="center"><strong>Codex CLI</strong> is a coding agent from OpenAI that runs locally on your computer.
<p align="center">
  <img src="https://github.com/openai/codex/blob/main/.github/codex-cli-splash.png" alt="Codex CLI splash" width="80%" />
</p>
</br>
If you want Codex in your code editor (VS Code, Cursor, Windsurf), <a href="https://developers.openai.com/codex/ide">install in your IDE.</a>
</br>If you want the desktop app experience, run <code>codex app</code> or visit <a href="https://chatgpt.com/codex?app-landing-page=true">the Codex App page</a>.
</br>If you are looking for the <em>cloud-based agent</em> from OpenAI, <strong>Codex Web</strong>, go to <a href="https://chatgpt.com/codex">chatgpt.com/codex</a>.</p>

---

## Quickstart

### Installing and running Codex CLI

Run the following on Mac or Linux to install Codex CLI:

```shell
curl -fsSL https://chatgpt.com/codex/install.sh | sh
```

Run the following on Windows to install Codex CLI:

```shell
powershell -ExecutionPolicy ByPass -c "irm https://chatgpt.com/codex/install.ps1 | iex"
```

The standalone installers download from `https://releases.openai.com/codex` by default and fall back to GitHub Releases if a metadata or asset download is unavailable. To force GitHub Releases, set `CODEX_INSTALLER_USE_RELEASES_OPENAI_COM` to `false` (`0` and `no` are also accepted):

```shell
curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_INSTALLER_USE_RELEASES_OPENAI_COM=false sh
```

```powershell
$env:CODEX_INSTALLER_USE_RELEASES_OPENAI_COM='false'; irm https://chatgpt.com/codex/install.ps1 | iex
```

Codex CLI can also be installed via the following package managers:

```shell
# Install using npm
npm install -g @openai/codex
```

```shell
# Install using Homebrew
brew install --cask codex
```

Then simply run `codex` to get started.

<details>
<summary>You can also go to the <a href="https://github.com/openai/codex/releases/latest">latest GitHub Release</a> and download the appropriate binary for your platform.</summary>

Each GitHub Release contains many executables, but in practice, you likely want one of these:

- macOS
  - Apple Silicon/arm64: `codex-aarch64-apple-darwin.tar.gz`
  - x86_64 (older Mac hardware): `codex-x86_64-apple-darwin.tar.gz`
- Linux
  - x86_64: `codex-x86_64-unknown-linux-musl.tar.gz`
  - arm64: `codex-aarch64-unknown-linux-musl.tar.gz`

Each archive contains a single entry with the platform baked into the name (e.g., `codex-x86_64-unknown-linux-musl`), so you likely want to rename it to `codex` after extracting it.

</details>

### Using Codex with your ChatGPT plan

Run `codex` and select **Sign in with ChatGPT**. We recommend signing into your ChatGPT account to use Codex as part of your Plus, Pro, Business, Edu, or Enterprise plan. [Learn more about what's included in your ChatGPT plan](https://help.openai.com/en/articles/11369540-codex-in-chatgpt).

You can also use Codex with an API key, but this requires [additional setup](https://developers.openai.com/codex/auth#sign-in-with-an-api-key).

## Docs

- [**Codex Documentation**](https://developers.openai.com/codex)
- [**Contributing**](./docs/contributing.md)
- [**Installing & building**](./docs/install.md)
- [**Open source fund**](./docs/open-source-fund.md)

This repository is licensed under the [Apache-2.0 License](LICENSE).
