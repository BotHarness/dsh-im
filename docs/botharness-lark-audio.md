# Checked Lark parent-audio candidate

Refs [BotHarness/DeepSeekBot#1023](https://github.com/BotHarness/DeepSeekBot/issues/1023).

The exclusive external Consumer can opt into `sourceVoiceAudio` alongside `sourceFiles`. A native user text request that mentions the receiving App and replies to a native `audio` message then carries that parent's exact voice descriptor: one `audio/unknown` attachment named `voice.opus` and `voice: { transcript: 'unavailable', itemId, durationMs? }`. Intake downloads no bytes. Lark supplies no transcript, so none is claimed; a missing native `duration` stays absent.

Resource reads re-fetch the request and its parent, compare the exact descriptor, and fetch the parent's `file_key` with `type=file`. Existing cancellation, consumer-lifetime and 25 MiB limits apply. A changed or missing parent fails with `stale-route`. The Provider does not decode or transcode; the consumer owns playback preparation. Feishu advertises `source-voice-audio-checked`.

## 中文

独占 external Consumer 在 `sourceFiles` 之外明确启用 `sourceVoiceAudio` 后，可关联“真正 @ 接收 App 的用户文本请求”所引用的原生 `audio` 语音：一个 `audio/unknown` 附件（名称 `voice.opus`）以及 `voice: { transcript: 'unavailable', itemId, durationMs? }`。入站不下载字节；飞书不提供转写，因此不声称有转写，平台未给出时长时保持缺失。

读取前重新核对原请求与父消息，比较完整资源描述，再以 `type=file` 获取父消息的 `file_key`；沿用取消、消费者生命周期和 25 MiB 限制，来源变化或缺失返回 `stale-route`。Provider 不解码、不转码，播放准备由消费者负责。飞书声明 `source-voice-audio-checked` 能力。
