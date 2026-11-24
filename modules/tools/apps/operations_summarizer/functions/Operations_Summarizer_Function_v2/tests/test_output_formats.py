"""Smoke tests for the audio and PDF output paths that the dependency bumps could affect."""

import edge_tts
import pytest
import tts_engine
from pdf_generation import generate_pdf_from_markdown_in_memory


def test_audio_is_mp3() -> None:
    assert tts_engine.get_audio_mime_type() == "audio/mpeg"
    assert tts_engine.get_audio_extension() == "mp3"


@pytest.mark.parametrize(("speed", "rate"), [(1.0, "+0%"), (1.2, "+20%"), (0.9, "-10%")])
def test_speed_is_formatted_as_an_edge_tts_rate(speed: float, rate: str) -> None:
    assert tts_engine._format_rate(speed) == rate


def test_generate_audio_joins_the_audio_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCommunicate:
        def __init__(self, text: str, voice: str, rate: str) -> None:
            self.rate = rate

        def stream_sync(self):
            yield {"type": "WordBoundary", "data": b"ignored"}
            yield {"type": "audio", "data": b"ab"}
            yield {"type": "audio", "data": b"cd"}

    monkeypatch.setattr(edge_tts, "Communicate", FakeCommunicate)

    assert tts_engine.generate_audio("Hello", speed=1.2) == b"abcd"


def test_generate_audio_skips_blank_text() -> None:
    assert tts_engine.generate_audio("   ") is None


def test_markdown_renders_to_a_pdf() -> None:
    pdf = generate_pdf_from_markdown_in_memory("# Summary\n\n- one\n- two\n", "summary.pdf")

    assert pdf.startswith(b"%PDF")
