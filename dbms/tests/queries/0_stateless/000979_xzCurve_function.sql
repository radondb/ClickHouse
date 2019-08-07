SELECT xzCurve(); -- { serverError 36 }
SELECT xzCurve('a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a'); -- { serverError 36 }
SELECT xzCurve(toDecimal64(1,1)); -- { serverError 36 }
SELECT xzCurve(toDecimal32(1,1)); -- { serverError 36 }
SELECT xzCurve(toDecimal128(1,1)); -- { serverError 36 }
SELECT xzCurve(toIntervalSecond(1)); -- { serverError 36 }
SELECT xzCurve(toIntervalMonth(100)); -- { serverError 36 }
SELECT xzCurve(toIntervalMinute(99)); -- { serverError 36 }
SELECT xzCurve(toIntervalHour(1)); -- { serverError 36 }
SELECT xzCurve(toIntervalWeek(1)); -- { serverError 36 }
SELECT xzCurve(toIntervalDay(1)); -- { serverError 36 }
SELECT xzCurve(toIntervalQuarter(1)); -- { serverError 36 }
SELECT xzCurve(toIntervalYear(1)); -- { serverError 36 }
SELECT xzCurve(toUUID(1)); -- { serverError 36 }
SELECT xzCurve(toString('a')); -- { serverError 36 }
SELECT hex(xzCurve(toUInt8(0), toUInt8(199)));
SELECT hex(xzCurve(toUInt8(0), toUInt8(191)));
SELECT hex(xzCurve(toUInt8(0), toUInt8(0)));
SELECT hex(xzCurve(toUInt8(0), toUInt16(255)));
SELECT hex(xzCurve(toUInt8(0), toUInt32(255)));
SELECT hex(xzCurve(toUInt8(0), toUInt64(255)));
SELECT hex(xzCurve(toUInt8(0), toUInt8(256)));
SELECT hex(xzCurve(toUInt8(255), toUInt8(0)));
SELECT hex(xzCurve(toDateTime(99999), toDate(1000000)));
SELECT hex(xzCurve(toDate(99999), toDate(10000)));
SELECT hex(xzCurve(toDateTime(99999), toDateTime(99)));
SELECT hex(xzCurve(CAST('hello', 'Enum8(\'hello\' = 1, \'world\' = 2)'), CAST('a', 'Enum8(\'a\' = 10, \'b\' = 11)')));
SELECT hex(xzCurve(CAST('hello', 'Enum16(\'hello\' = 1, \'world\' = 2)'), CAST('a', 'Enum16(\'a\' = 10, \'b\' = 11)')));
SELECT hex(xzCurve(CAST('hello', 'Enum8(\'hello\' = -11, \'world\' = 2)'), CAST('a', 'Enum8(\'a\' = -30, \'b\' = 11)')));
SELECT hex(xzCurve(CAST('hello', 'Enum16(\'hello\' = -1, \'world\' = 2)'), CAST('a', 'Enum16(\'a\' = -10, \'b\' = 11)')));
SELECT hex(xzCurve(CAST('hello', 'Enum8(\'hello\' = 1, \'world\' = 2)'), CAST('a', 'Enum16(\'a\' = 10, \'b\' = 11)')));
SELECT hex(xzCurve(toFloat32(99999.2), toFloat32(99999.9)));
SELECT hex(xzCurve(toFloat64(99999.2), toFloat64(99999.9)));
