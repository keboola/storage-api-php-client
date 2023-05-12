<?php

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class TablesDefinitionCommonTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testTooManyColumnsCountFailed(): void
    {
        $this->skipTestForBackend([
            self::BACKEND_REDSHIFT,
        ], 'Backend does not support table-definition feature');
        $this->skipTestForBackend([
            self::BACKEND_EXASOL,
            self::BACKEND_BIGQUERY,
        ], 'Backend has no limit regarding number of columns');

        $data = [
            'name' => 'tooManyColumns',
            'primaryKeysNames' => [],
            'columns' => [],
        ];
        // generate table definition with too many columns - over limit on every backend
        // don't make that number ridiculously big. RequestObject won't make it...
        for ($i = 1; $i <= 2100; $i++) {
            $data['columns'][] = [
                'name' => 'col' . $i,
                'basetype' => 'BOOLEAN',
            ];
        }
        try {
            $this->_client->createTableDefinition(
                $this->getTestBucketId(),
                $data,
            );
            $this->fail('There were 2100 columns which should fail.');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.definitionValidation.tooManyColumns', $e->getStringCode());
        }
    }
}
