<?php

namespace Keboola\Test\Utils;

trait MetadataUtils
{
    /**
     * @param array{key: string, value: string} $expected
     * @param array{id: string|numeric, key: string, value: string, timestamp: string} $actual
     * @return void
     */
    private function assertMetadataEquals(array $expected, array $actual)
    {
        foreach ($expected as $key => $value) {
            self::assertArrayHasKey($key, $actual);
            self::assertSame($value, $actual[$key]);
        }
        self::assertArrayHasKey('timestamp', $actual);
    }
}
