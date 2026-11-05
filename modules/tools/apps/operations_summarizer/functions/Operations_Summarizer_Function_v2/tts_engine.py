"""
Text-to-Speech engine for Podcast Generation Function.

Uses ``edge-tts`` (Microsoft Edge Read Aloud) for high-quality neural TTS.
All imports are lazy so they don't slow down function startup.

Output format is MP3 (audio/mpeg).
"""

from __future__ import annotations

AUDIO_MIME_TYPE = "audio/mpeg"
AUDIO_EXTENSION = "mp3"

DEFAULT_VOICE = "en-US-AndrewMultilingualNeural"


def generate_audio(
    text: str,
    voice: str = DEFAULT_VOICE,
    speed: float = 1.0,
) -> bytes | None:
    """Generate MP3 audio bytes from text using Microsoft Edge TTS.

    Parameters
    ----------
    text : str
        The text to synthesize.
    voice : str
        Edge TTS voice name (default: ``en-US-AndrewMultilingualNeural``).
    speed : float
        Speed multiplier 0.5–2.0 (default: 1.0).

    Returns
    -------
    bytes or None
        MP3 audio bytes on success, None on failure.
    """
    if not text or not text.strip():
        print("  TTS: empty text, skipping")
        return None

    rate_str = _format_rate(speed)
    print(f"  TTS: using edge-tts (voice={voice}, rate={rate_str})")

    try:
        import edge_tts

        communicate = edge_tts.Communicate(text, voice, rate=rate_str)
        chunks: list[bytes] = []

        for chunk in communicate.stream_sync():
            if chunk["type"] == "audio":
                chunks.append(chunk["data"])

        if not chunks:
            print("  TTS: no audio chunks received")
            return None

        audio_bytes = b"".join(chunks)
        print(f"  TTS: generated {len(audio_bytes)} bytes of MP3 audio")
        return audio_bytes

    except Exception as e:
        print(f"  TTS error: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_audio_mime_type() -> str:
    """Return the MIME type of generated audio."""
    return AUDIO_MIME_TYPE


def get_audio_extension() -> str:
    """Return the file extension for generated audio."""
    return AUDIO_EXTENSION


def _format_rate(speed: float) -> str:
    """Convert speed multiplier to edge-tts rate string (e.g. '+20%' or '-10%')."""
    pct = round((speed - 1.0) * 100)
    if pct >= 0:
        return f"+{pct}%"
    return f"{pct}%"
