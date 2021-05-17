<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class ImportTypedTableTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->fail(sprintf('Tables definition feature is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableDefinition()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $data = [
            'name' => 'tw-accounts',
            'primaryKeysNames' => ['id'],
            'columns' => [
                ['name' => 'id', 'definition' => ['type' => 'INT']],
                ['name' => 'idTwitter', 'definition' => ['type' => 'BIGINT']],
                ['name' => 'name', 'definition' => ['type' => 'NVARCHAR']],
                ['name' => 'import', 'definition' => ['type' => 'INT']],
                ['name' => 'isImported', 'definition' => ['type' => 'TINYINT']],
                ['name' => 'apiLimitExceededDatetime', 'definition' => ['type' => 'DATETIME2']],
                ['name' => 'analyzeSentiment', 'definition' => ['type' => 'TINYINT']],
                ['name' => 'importKloutScore', 'definition' => ['type' => 'INT']],
                ['name' => 'timestamp', 'definition' => ['type' => 'DATETIME2']],
                ['name' => 'oauthToken', 'definition' => ['type' => 'NVARCHAR']],
                ['name' => 'oauthSecret', 'definition' => ['type' => 'NVARCHAR']],
                ['name' => 'idApp', 'definition' => ['type' => 'INT']],
            ],
            'distribution' => [
                'type' => 'HASH',
                'distributionColumnsNames' => ['id'],
            ],
        ];
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, $data);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        try {
            // try to import table with more columns
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile(__DIR__ . '/../../_data/tw_accounts.more-cols.csv'),
                [
                    'incremental' => true,
                ]
            );
        } catch (ClientException $e) {
            $this->assertSame('During the import of typed tables new columns can\'t be added. Columns indwelling in import are "secret".', $e->getMessage());
            $this->assertSame('csvImport.columnsNotMatch', $e->getStringCode());
        }

        // import data using full load
        $fullLoadInputFile= __DIR__ . '/../../_data/tw_accounts.csv';
        $fullLoadExpectationFile= __DIR__ . '/../../_data/tw_accounts.expectation.full.csv';
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($fullLoadInputFile),
            [
                'incremental' => false,
            ]
        );

        $this->assertLinesEqualsSorted(file_get_contents($fullLoadExpectationFile), $this->_client->getTableDataPreview($tableId, [
            'format' => 'rfc',
        ]), 'imported data comparsion');

        // import data using incremental load
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/tw_accounts.increment.csv'),
            [
                'incremental' => true,
            ]
        );

        $this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../../_data/tw_accounts.expectation.increment.csv'), $this->_client->getTableDataPreview($tableId, [
            'format' => 'rfc',
        ]), 'imported data comparsion');
    }
}
