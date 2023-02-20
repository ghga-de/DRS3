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
"""Basic range parsing, adding envelope offset"""


from typing import Tuple


class RangeParsingError(RuntimeError):
    """TODO"""


class EmptyRangeError(RangeParsingError):
    """TODO"""


class InvalidFormatError(RangeParsingError):
    """TODO"""


class NegativeRangeError(RangeParsingError):
    """TODO"""


class UnsupportedUnitTypeError(RangeParsingError):
    """TODO"""


def parse_header(range_header: str, offset: int) -> Tuple[int, int]:
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
        # we only support returning the first range
        source_ranges = ranges.split(",")
        return _parse_single_range(source_ranges[0], offset)

    return _parse_single_range(ranges, offset)


def _parse_single_range(byte_range: str, offset: int):
    """TODO"""
    start, _, end = byte_range.strip().partition("-")

    if start.isnumeric():
        range_start = int(start) + offset
    if end.isnumeric():
        range_end = int(end) + offset

    if not any([range_start, range_end]):
        raise EmptyRangeError()

    if all([range_start, range_end]) and range_end - range_start < 0:
        raise NegativeRangeError()

    return range_start, range_end
