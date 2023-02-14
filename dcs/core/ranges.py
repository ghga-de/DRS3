# Copyright 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Tuple


class RangeParsingError(RuntimeError):
    ...


class InvalidFormatError(RangeParsingError):
    ...


class EmptyOrNegativeRangeError(RangeParsingError):
    ...


class OverlappingRangesError(RangeParsingError):
    ...


class UnsupportedUnitTypeError(RangeParsingError):
    ...


def parse_ranges(range_header: str, offset: int) -> list[Tuple(int, int)]:
    """
    Range: <unit>=<range-start>-
    Range: <unit>=<range-start>-<range-end>
    Range: <unit>=-<suffix-length>
    """
    if "=" not in range_header:
        raise InvalidFormatError(range_header)

    unit, _, ranges = range_header.partition("=")

    if unit != "bytes":
        raise UnsupportedUnitTypeError(unit)

    if "," in ranges:
        parsed_ranges = []
        for byte_range in ranges.split(","):
            parsed_ranges.append(_parse_single_range(byte_range, offset))
        _validate_ranges(parsed_ranges)
        return parsed_ranges
    else:
        range_start, range_end = _parse_single_range(ranges, offset)
        return [(range_start, range_end)]


def _parse_single_range(byte_range: str, offset: int):
    """TODO"""
    start, _, end = byte_range.strip().partition("-")

    start, end = start.strip(), end.strip()

    if start:
        start = int(start) + offset
    if end:
        end = int(end) + offset

    if not any(start, end):
        raise RangeParsingError()

    if end - start <= 0:
        raise EmptyOrNegativeRangeError()

    return start, end


def _validate_ranges(parsed_ranges: list[Tuple[int, int]]):
    """TODO"""
    seen_ends = []
    processed_ranges = []
    for start, end in parsed_ranges:
        if isinstance(start, str) or isinstance(end, str):
            raise RangeParsingError()
        if seen_ends:
            for seen_end in seen_ends:
                if start <= seen_end:
                    raise OverlappingRangesError()
        processed_ranges.append(f"{start}-{end}")
