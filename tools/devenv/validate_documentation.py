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

import io
import re
import struct
import sys
import zlib
from pathlib import Path
from urllib.parse import unquote, urlsplit

from defusedxml import ElementTree
from docutils import nodes
from docutils.core import publish_doctree
from docutils.utils import SystemMessage

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
README = REPOSITORY_ROOT / "etc/docker/dev/README.rst"
MEDIA_DIR = README.parent / "docs/images"

MAX_IMAGE_BYTES = 5 * 1024 * 1024
MAX_MEDIA_BYTES = 15 * 1024 * 1024
MIN_RASTER_WIDTH = 3840
MIN_RASTER_HEIGHT = 2160

_PERSONAL_CONTENT = re.compile(
    r"(?:/Users/[^/\s]+/|/home/[^/\s]+/|[A-Za-z]:\\Users\\[^\\\s]+\\"
    r"|\bdeveloper-toolbox-squashed\b|\btesting-refactor\b|\bxreview/[^\s\"'<>]+)",
    re.IGNORECASE,
)
_ALLOWED_EXTERNAL_SCHEMES = {"http", "https", "mailto"}
_FORBIDDEN_SVG_ELEMENTS = {
    "animate",
    "animatemotion",
    "animatetransform",
    "embed",
    "foreignobject",
    "iframe",
    "object",
    "script",
    "set",
}
_CSS_URL = re.compile(r"url\(\s*(['\"]?)(.*?)\1\s*\)", re.IGNORECASE)

_PNG_CHANNELS = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}
_PNG_BIT_DEPTHS = {
    0: {1, 2, 4, 8, 16},
    2: {8, 16},
    3: {1, 2, 4, 8},
    4: {8, 16},
    6: {8, 16},
}
_PNG_ADAM7 = (
    (0, 0, 8, 8),
    (4, 0, 8, 8),
    (0, 4, 4, 8),
    (2, 0, 4, 4),
    (0, 2, 2, 4),
    (1, 0, 2, 2),
    (0, 1, 1, 2),
)
_MAX_DECODED_RASTER_BYTES = 256 * 1024 * 1024


def _zlib_decompress(data: bytes, limit: int) -> bytes:
    decompressor = zlib.decompressobj()
    try:
        decoded = decompressor.decompress(data, limit + 1)
    except zlib.error as error:
        raise ValueError("PNG contains invalid compressed image data") from error
    if (
        len(decoded) > limit
        or decompressor.unconsumed_tail
        or not decompressor.eof
        or decompressor.unused_data
    ):
        raise ValueError("PNG contains invalid or oversized compressed image data")
    return decoded


def _png_scanlines(
    width: int,
    height: int,
    bits_per_pixel: int,
    interlace: int,
) -> list[tuple[int, int]]:
    if interlace == 0:
        passes = ((0, 0, 1, 1),)
    else:
        passes = _PNG_ADAM7

    scanlines = []
    for x_start, y_start, x_step, y_step in passes:
        pass_width = max(0, (width - x_start + x_step - 1) // x_step)
        pass_height = max(0, (height - y_start + y_step - 1) // y_step)
        if pass_width and pass_height:
            scanlines.append(((pass_width * bits_per_pixel + 7) // 8, pass_height))
    return scanlines


def _png_details(data: bytes) -> tuple[int, int, bytes]:
    if not data.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError("content is not PNG")

    offset = 8
    width = height = 0
    bit_depth = color_type = interlace = 0
    metadata = bytearray()
    compressed = bytearray()
    seen_ihdr = seen_plte = seen_idat = seen_iend = False
    idat_closed = False
    while offset < len(data):
        if offset + 12 > len(data):
            raise ValueError("PNG contains a truncated chunk")
        length = struct.unpack_from(">I", data, offset)[0]
        chunk_type = data[offset + 4:offset + 8]
        if len(chunk_type) != 4 or not all(chr(value).isalpha() and value < 128 for value in chunk_type):
            raise ValueError("PNG contains an invalid chunk type")
        chunk_start = offset + 8
        chunk_end = chunk_start + length
        if chunk_end + 4 > len(data):
            raise ValueError("PNG contains a truncated chunk")
        chunk = data[chunk_start:chunk_end]
        expected_crc = struct.unpack_from(">I", data, chunk_end)[0]
        actual_crc = zlib.crc32(chunk, zlib.crc32(chunk_type)) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise ValueError("PNG contains an invalid chunk checksum")

        if not seen_ihdr and chunk_type != b"IHDR":
            raise ValueError("PNG does not start with IHDR")
        if chunk_type == b"IHDR":
            if seen_ihdr or len(chunk) != 13:
                raise ValueError("PNG contains an invalid IHDR")
            width, height, bit_depth, color_type, compression, filtering, interlace = struct.unpack(
                ">IIBBBBB",
                chunk,
            )
            if (
                not width
                or not height
                or color_type not in _PNG_CHANNELS
                or bit_depth not in _PNG_BIT_DEPTHS[color_type]
                or compression != 0
                or filtering != 0
                or interlace not in {0, 1}
            ):
                raise ValueError("PNG contains unsupported or invalid IHDR values")
            seen_ihdr = True
        elif chunk_type == b"PLTE":
            if seen_plte or seen_idat or not length or length % 3 or length > 768:
                raise ValueError("PNG contains an invalid PLTE")
            if color_type == 3 and length // 3 > 2**bit_depth:
                raise ValueError("PNG palette exceeds its indexed bit depth")
            seen_plte = True
        elif chunk_type == b"IDAT":
            if idat_closed or (color_type == 3 and not seen_plte):
                raise ValueError("PNG contains misplaced IDAT data")
            seen_idat = True
            compressed.extend(chunk)
        elif chunk_type == b"IEND":
            if length or not seen_idat:
                raise ValueError("PNG contains an invalid IEND")
            seen_iend = True
        elif chunk_type in {b"tEXt", b"iTXt"}:
            metadata.extend(chunk)
        elif chunk_type[0] & 0x20 == 0:
            raise ValueError(f"PNG contains an unknown critical chunk {chunk_type.decode('ascii')}")

        if seen_idat and chunk_type not in {b"IDAT", b"IEND"}:
            idat_closed = True
        offset = chunk_end + 4
        if chunk_type == b"IEND":
            if offset != len(data):
                raise ValueError("PNG contains data after IEND")
            break

    if not seen_iend:
        raise ValueError("PNG has no complete IEND chunk")

    scanlines = _png_scanlines(
        width,
        height,
        _PNG_CHANNELS[color_type] * bit_depth,
        interlace,
    )
    expected_size = sum((row_bytes + 1) * rows for row_bytes, rows in scanlines)
    if expected_size > _MAX_DECODED_RASTER_BYTES:
        raise ValueError("PNG decoded image data exceeds the safety limit")
    decoded = _zlib_decompress(bytes(compressed), expected_size)
    if len(decoded) != expected_size:
        raise ValueError("PNG decompressed image data has the wrong size")
    decoded_offset = 0
    for row_bytes, rows in scanlines:
        for _row in range(rows):
            if decoded[decoded_offset] > 4:
                raise ValueError("PNG contains an invalid scanline filter")
            decoded_offset += row_bytes + 1
    return width, height, bytes(metadata)


def _svg_details(data: bytes) -> tuple[int, int, bytes]:
    try:
        source = data.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError("SVG must use UTF-8 encoding") from error
    processing_instruction_source = source
    if source.startswith("<?xml "):
        declaration_end = source.find("?>")
        if declaration_end < 0:
            raise ValueError("SVG contains an incomplete XML declaration")
        processing_instruction_source = source[declaration_end + 2:]
    if "<?" in processing_instruction_source:
        raise ValueError("SVG contains a processing instruction")
    try:
        root = ElementTree.fromstring(source)
    except ElementTree.ParseError as error:
        raise ValueError(f"content is not valid SVG: {error}") from error
    if root.tag.rsplit("}", 1)[-1] != "svg":
        raise ValueError("XML root element is not SVG")

    root_children = {
        child.tag.rsplit("}", 1)[-1].lower()
        for child in root
        if (child.text or "").strip()
    }
    if not {"title", "desc"} <= root_children:
        raise ValueError("SVG needs non-empty title and desc elements")

    for element in root.iter():
        element_name = element.tag.rsplit("}", 1)[-1].lower()
        if element_name in _FORBIDDEN_SVG_ELEMENTS:
            raise ValueError(f"SVG contains a forbidden element: {element_name}")
        if element_name == "style" and re.search(r"@import\b", element.text or "", re.IGNORECASE):
            raise ValueError("SVG contains an external CSS import")
        css_values = [element.text or ""] if element_name == "style" else []
        for name, value in element.attrib.items():
            local_name = name.rsplit("}", 1)[-1].lower()
            if re.fullmatch(r"on[a-z]+", local_name):
                raise ValueError(f"SVG contains an event handler: {local_name}")
            if local_name == "href" and value.strip() and not value.strip().startswith("#"):
                raise ValueError("SVG contains an external href")
            css_values.append(value)
        for css in css_values:
            for match in _CSS_URL.finditer(css):
                if not match.group(2).strip().startswith("#"):
                    raise ValueError("SVG contains an external CSS URL")

    view_box = root.get("viewBox", "").replace(",", " ").split()
    if len(view_box) == 4:
        width, height = float(view_box[2]), float(view_box[3])
    else:
        try:
            width = float(root.get("width", "").removesuffix("px"))
            height = float(root.get("height", "").removesuffix("px"))
        except ValueError as error:
            raise ValueError("SVG needs numeric dimensions or a viewBox") from error
    if width <= 0 or height <= 0:
        raise ValueError("SVG has no valid dimensions")
    return round(width), round(height), data


def _image_details(path: Path) -> tuple[int, int, bytes]:
    data = path.read_bytes()
    suffix = path.suffix.lower()
    if suffix == ".png":
        return _png_details(data)
    if suffix == ".svg":
        return _svg_details(data)
    if suffix in {".gif", ".jpeg", ".jpg", ".webp"}:
        raise ValueError("only static PNG and SVG documentation images are supported")
    raise ValueError(f"unsupported image type {suffix or '<none>'}")


def _local_path(uri: str, source: Path, repository_root: Path) -> Path | None:
    parsed = urlsplit(uri)
    scheme = parsed.scheme.lower()
    if scheme == "file":
        raise ValueError("file URI is not permitted")
    if scheme:
        if scheme not in _ALLOWED_EXTERNAL_SCHEMES:
            raise ValueError(f"unsupported external URI scheme: {scheme}")
        return None
    if parsed.netloc:
        raise ValueError("network-path link must use an explicit HTTP or HTTPS scheme")
    if not parsed.path:
        return None
    path = Path(unquote(parsed.path))
    if path.is_absolute():
        raise ValueError("absolute local link is not permitted")
    resolved = (source.parent / path).resolve()
    if not resolved.is_relative_to(repository_root):
        raise ValueError("local link leaves the repository root")
    return resolved


def validate_documentation(readme: Path = README, media_dir: Path = MEDIA_DIR) -> list[str]:
    errors: list[str] = []
    repository_root = REPOSITORY_ROOT if readme.resolve().is_relative_to(REPOSITORY_ROOT) else readme.parent.resolve()
    source = readme.read_text(encoding="utf-8")
    personal_match = _PERSONAL_CONTENT.search(source)
    if personal_match:
        errors.append(f"{readme}: contains personal path or experimental branch string {personal_match.group(0)!r}")

    warning_stream = io.StringIO()
    try:
        document = publish_doctree(
            source,
            source_path=str(readme),
            settings_overrides={"halt_level": 2, "report_level": 2, "warning_stream": warning_stream},
        )
    except SystemMessage as error:
        details = warning_stream.getvalue().strip() or str(error)
        return [f"{readme}: strict RST parsing failed: {details}", *errors]

    local_images: set[Path] = set()
    for node in document.findall((nodes.image, nodes.reference)):
        if isinstance(node, nodes.image) and not node.get("alt", "").strip():
            errors.append(f"{readme}: image has no alternative text: {node.get('uri', '<unknown>')}")
        uri = node.get("uri") if isinstance(node, nodes.image) else node.get("refuri")
        if not uri:
            continue
        if uri.startswith("#"):
            if uri[1:] not in document.ids:
                errors.append(f"{readme}: local target does not exist: {uri}")
            continue
        try:
            target = _local_path(uri, readme, repository_root)
        except ValueError as error:
            errors.append(f"{readme}: invalid local link {uri!r}: {error}")
            continue
        if target is None:
            if isinstance(node, nodes.image):
                errors.append(f"{readme}: image must be repository-local: {uri}")
            continue
        if not target.exists():
            errors.append(f"{readme}: local link does not exist: {uri}")
        elif isinstance(node, nodes.image):
            local_images.add(target)

    media = {path.resolve() for path in media_dir.iterdir() if path.is_file()}
    media.update(local_images)
    total_size = sum(path.stat().st_size for path in media if path.exists())
    if total_size > MAX_MEDIA_BYTES:
        errors.append(f"{media_dir}: media totals {total_size} bytes; limit is {MAX_MEDIA_BYTES}")

    for path in sorted(media):
        if not path.exists():
            continue
        size = path.stat().st_size
        if size > MAX_IMAGE_BYTES:
            errors.append(f"{path}: image is {size} bytes; limit is {MAX_IMAGE_BYTES}")
            continue
        try:
            width, height, metadata = _image_details(path)
        except ValueError as error:
            errors.append(f"{path}: {error}")
            continue
        if path.suffix.lower() != ".svg" and (width < MIN_RASTER_WIDTH or height < MIN_RASTER_HEIGHT):
            errors.append(
                f"{path}: raster dimensions are {width}x{height}; minimum is "
                f"{MIN_RASTER_WIDTH}x{MIN_RASTER_HEIGHT}"
            )
        personal_match = _PERSONAL_CONTENT.search(metadata.decode("latin-1", errors="ignore"))
        if personal_match:
            errors.append(f"{path}: metadata contains personal path or experimental branch string {personal_match.group(0)!r}")
    return errors


def main() -> int:
    errors = validate_documentation()
    if errors:
        print("\n".join(f"ERROR: {error}" for error in errors), file=sys.stderr)
        return 1
    print(f"Validated {README} and {MEDIA_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
