# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import struct
import zlib
from typing import TYPE_CHECKING

import pytest

validator = pytest.importorskip("tools.devenv.validate_documentation")

if TYPE_CHECKING:
    from pathlib import Path


def _png(width: int = 3840, height: int = 2160, metadata: bytes = b"") -> bytes:
    def chunk(kind: bytes, payload: bytes) -> bytes:
        checksum = zlib.crc32(kind + payload) & 0xFFFFFFFF
        return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", checksum)

    ihdr = struct.pack(">IIBBBBB", width, height, 1, 3, 0, 0, 0)
    chunks = [chunk(b"IHDR", ihdr), chunk(b"PLTE", b"\0\0\0\xff\xff\xff")]
    if metadata:
        chunks.append(chunk(b"tEXt", b"Source\0" + metadata))
    scanline = b"\0" + bytes((width + 7) // 8)
    chunks.append(chunk(b"IDAT", zlib.compress(scanline * height)))
    chunks.append(chunk(b"IEND", b""))
    return b"\x89PNG\r\n\x1a\n" + b"".join(chunks)


def _write_guide(tmp_path: Path, body: str, image: bytes | None = None) -> tuple[Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    readme = tmp_path / "README.rst"
    readme.write_text(body)
    media = tmp_path / "images"
    media.mkdir()
    if image is not None:
        (media / "capture.png").write_bytes(image)
    return readme, media


def test_validates_rst_links_and_media(tmp_path: Path) -> None:
    readme, media = _write_guide(
        tmp_path,
        """Guide
=====

.. _details:

Details
-------

`Details <#details>`__ and `Rucio <https://rucio.cern.ch>`_.

.. image:: images/capture.png
   :alt: Example capture
""",
        _png(),
    )

    assert validator.validate_documentation(readme, media) == []


def test_rejects_invalid_rst_and_missing_targets(tmp_path: Path) -> None:
    invalid, media = _write_guide(tmp_path / "invalid", "Guide\n=====\n\n.. unknown-directive::\n")
    errors = validator.validate_documentation(invalid, media)
    assert any("strict RST parsing failed" in error for error in errors)

    missing, media = _write_guide(
        tmp_path / "missing",
        """Guide
=====

`Missing section <#missing>`__.

.. image:: images/missing.png
""",
    )
    errors = validator.validate_documentation(missing, media)
    assert any("local target does not exist: #missing" in error for error in errors)
    assert any("local link does not exist: images/missing.png" in error for error in errors)


def test_rejects_unsupported_or_low_resolution_media(tmp_path: Path) -> None:
    readme, media = _write_guide(tmp_path, "Guide\n=====\n", _png(1920, 1080))
    (media / "walkthrough.webp").write_bytes(b"RIFF\x00\x00\x00\x00WEBP")
    (media / "capture.jpg").write_bytes(b"\xff\xd8\xff\xd9")
    (media / "animation.gif").write_bytes(b"GIF89a")

    errors = validator.validate_documentation(readme, media)

    assert any("minimum is 3840x2160" in error for error in errors)
    assert sum("only static PNG and SVG" in error for error in errors) == 3


def test_rejects_mismatched_or_oversized_media(tmp_path: Path) -> None:
    readme, media = _write_guide(tmp_path, "Guide\n=====\n")
    (media / "mismatch.png").write_bytes(b"\xff\xd8\xff\xd9")
    (media / "oversized.png").write_bytes(_png().ljust(validator.MAX_IMAGE_BYTES + 1, b"\0"))

    errors = validator.validate_documentation(readme, media)

    assert any("mismatch.png: content is not PNG" in error for error in errors)
    assert any("oversized.png: image is" in error for error in errors)


def test_rejects_truncated_or_corrupt_png_data(tmp_path: Path) -> None:
    readme, media = _write_guide(tmp_path, "Guide\n=====\n")
    valid = _png()
    (media / "truncated.png").write_bytes(valid[:-12])
    corrupt = bytearray(valid)
    idat = corrupt.index(b"IDAT")
    corrupt[idat + 4] ^= 0x01
    (media / "corrupt.png").write_bytes(corrupt)
    malformed = bytearray(valid)
    idat = malformed.index(b"IDAT")
    idat_length = struct.unpack_from(">I", malformed, idat - 4)[0]
    idat_end = idat + 4 + idat_length
    malformed[idat + 4:idat_end] = bytes(idat_length)
    struct.pack_into(
        ">I",
        malformed,
        idat_end,
        zlib.crc32(malformed[idat:idat_end]) & 0xFFFFFFFF,
    )
    (media / "malformed.png").write_bytes(malformed)

    errors = validator.validate_documentation(readme, media)

    assert any("truncated.png: PNG has no complete IEND chunk" in error for error in errors)
    assert any("corrupt.png: PNG contains an invalid chunk checksum" in error for error in errors)
    assert any("malformed.png: PNG contains invalid compressed image data" in error for error in errors)


def test_rejects_active_or_external_svg_content(tmp_path: Path) -> None:
    readme, media = _write_guide(tmp_path, "Guide\n=====\n")
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 3840 2160">'
        '<title>Diagram</title><desc>Accessible description</desc>{}</svg>'
    )
    (media / "script.svg").write_text(svg.format("<script>alert(1)</script>"))
    (media / "event.svg").write_text(svg.format('<rect onload="alert(1)"/>'))
    (media / "external.svg").write_text(svg.format('<image href="https://example.com/capture.png"/>'))
    (media / "css.svg").write_text(svg.format('<rect style="fill:url(https://example.com/a.svg)"/>'))
    (media / "foreign.svg").write_text(svg.format("<foreignObject><div>HTML</div></foreignObject>"))
    (media / "motion.svg").write_text(svg.format('<animate attributeName="x" from="0" to="1"/>'))
    (media / "stylesheet.svg").write_text(
        '<?xml-stylesheet href="https://example.com/theme.css"?>' + svg.format("<rect/>")
    )
    (media / "unlabelled.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 3840 2160"><rect/></svg>'
    )

    errors = validator.validate_documentation(readme, media)

    assert any("script.svg: SVG contains a forbidden element: script" in error for error in errors)
    assert any("event.svg: SVG contains an event handler: onload" in error for error in errors)
    assert any("external.svg: SVG contains an external href" in error for error in errors)
    assert any("css.svg: SVG contains an external CSS URL" in error for error in errors)
    assert any("foreign.svg: SVG contains a forbidden element: foreignobject" in error for error in errors)
    assert any("motion.svg: SVG contains a forbidden element: animate" in error for error in errors)
    assert any("stylesheet.svg: SVG contains a processing instruction" in error for error in errors)
    assert any("unlabelled.svg: SVG needs non-empty title and desc elements" in error for error in errors)


def test_rejects_unlabelled_or_external_images_and_unsafe_links(tmp_path: Path) -> None:
    readme, media = _write_guide(
        tmp_path,
        """Guide
=====

`Unsupported <ftp://example.com/file>`_.

.. image:: images/capture.png

.. image:: https://example.com/remote.png
   :alt: Remote image
""",
        _png(),
    )

    errors = validator.validate_documentation(readme, media)

    assert any("unsupported external URI scheme: ftp" in error for error in errors)
    assert any("image has no alternative text: images/capture.png" in error for error in errors)
    assert any("image must be repository-local: https://example.com/remote.png" in error for error in errors)


def test_rejects_absolute_or_out_of_repository_links(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("outside")
    readme, media = _write_guide(
        tmp_path,
        """Guide
=====

`Absolute </etc/passwd>`_.
`File URI <file:///etc/passwd>`_.
`Escape <../outside.txt>`_.
""",
    )

    errors = validator.validate_documentation(readme, media)

    assert any("absolute local link is not permitted" in error for error in errors)
    assert any("file URI is not permitted" in error for error in errors)
    assert any("local link leaves the repository root" in error for error in errors)


def test_rejects_personal_content_in_text_and_image_metadata(tmp_path: Path) -> None:
    readme, media = _write_guide(
        tmp_path,
        "Guide\n=====\n\nBuilt from developer-toolbox-squashed.\n",
        _png(metadata=b"source=/Users/alice/project"),
    )

    errors = validator.validate_documentation(readme, media)

    assert sum("personal path or experimental branch" in error for error in errors) == 2


def test_developer_toolbox_workflow_runs_documentation_validation() -> None:
    workflow = (validator.REPOSITORY_ROOT / ".github/workflows/developer_toolbox.yml").read_text()
    push_trigger = workflow.split("  push:\n", 1)[1].split("  workflow_dispatch:\n", 1)[0]

    assert "defusedxml==0.7.1 docutils==0.23" in workflow
    assert "pygments==2.20.0" in workflow
    assert "fetch-depth: 2" in workflow
    assert "python tools/devenv/validate_upstream_ci_contract.py" in workflow
    assert "python tools/devenv/validate_documentation.py" in workflow
    assert "developer-toolbox-squashed" in push_trigger
    assert "paths:" not in push_trigger
