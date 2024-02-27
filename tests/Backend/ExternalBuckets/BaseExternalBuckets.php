<?php

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\MetadataUtils;

abstract class BaseExternalBuckets extends StorageApiTestCase
{
    use MetadataUtils;

    protected string $thisBackend;

    protected function assertColumnMetadata(
        string $expectedType,
        string $expectedNullable,
        string $expectedBasetype,
        ?string $expectedLength,
        array $columnsMetadata
    ): void {
        $metadataToCompare = [];
        foreach ($columnsMetadata as $columnMetadata) {
            $metadataToCompare[$columnMetadata['key']] = $columnMetadata;
        }

        $this->assertSingleMetadataEntry($metadataToCompare['KBC.datatype.type'], $expectedType, 'KBC.datatype.type');
        $this->assertSingleMetadataEntry($metadataToCompare['KBC.datatype.nullable'], $expectedNullable, 'KBC.datatype.nullable');
        $this->assertSingleMetadataEntry($metadataToCompare['KBC.datatype.basetype'], $expectedBasetype, 'KBC.datatype.basetype');

        if ($expectedLength !== null) {
            $this->assertSingleMetadataEntry($metadataToCompare['KBC.datatype.length'], $expectedLength, 'KBC.datatype.length');
        }
    }

    protected function setRunId(): string
    {
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        return $runId;
    }
}
