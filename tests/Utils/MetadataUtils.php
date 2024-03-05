<?php

namespace Keboola\Test\Utils;

trait MetadataUtils
{
    /**
     * @param array{key: string, value: string} $expected
     * @param array{id: string|numeric, key: string, value: string, timestamp: string} $actual
     * @return void
     */
    private function assertMetadataEquals(array $expected, array $actual): void
    {
        foreach ($expected as $key => $value) {
            self::assertArrayHasKey($key, $actual);
            self::assertSame($value, $actual[$key]);
        }
        self::assertArrayHasKey('timestamp', $actual);
    }

    protected function assertSingleMetadataEntry(array $metadataEntry, string $expectedValue, string $key, ?string $provider = 'storage'): void
    {
        $this->assertArrayEqualsExceptKeys([
            'key' => $key,
            'value' => $expectedValue,
            'provider' => $provider,
        ], $metadataEntry, ['id', 'timestamp']);
    }
}
