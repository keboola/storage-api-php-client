<?php

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;

abstract class BaseExternalBuckets extends StorageApiTestCase
{
    protected string $thisBackend;
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }


    protected function assertColumnMetadata(
        string $expectedType,
        string $expectedNullable,
        string $expectedBasetype,
        ?string $expectedLength,
        array $columnMetadata
    ): void {
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => $expectedType,
            'provider' => 'storage',
        ], $columnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => $expectedNullable,
            'provider' => 'storage',
        ], $columnMetadata[1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => $expectedBasetype,
            'provider' => 'storage',
        ], $columnMetadata[2], ['id', 'timestamp']);

        if ($expectedLength !== null) {
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.length',
                'value' => $expectedLength,
                'provider' => 'storage',
            ], $columnMetadata[3], ['id', 'timestamp']);
        }
    }

    protected function setRunId(): string
    {
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        return $runId;
    }
}
