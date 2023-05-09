<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class ImportTypedTableTest extends ParallelWorkspacesTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableDefinition(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $payload = [
            'name' => 'tw-accountsFull',
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

        $tableId = $this->_client->createTableDefinition($bucketId, $payload);

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
            $this->fail('Fullload into table having datatypes defined should fail.');
        } catch (ClientException $e) {
            $this->assertSame('During the import of typed tables new columns can\'t be added. Extra columns found: "secret".', $e->getMessage());
            $this->assertSame('csvImport.columnsNotMatch', $e->getStringCode());
        }

        // import data using full load
        $fullLoadInputFile = __DIR__ . '/../../_data/tw_accounts.csv';
        $fullLoadExpectationFile = __DIR__ . '/../../_data/tw_accounts.expectation.full.csv';
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($fullLoadInputFile),
            [
                'incremental' => false,
            ]
        );

        //$this->assertLinesEqualsSorted(file_get_contents($fullLoadExpectationFile), $this->_client->getTableDataPreview($tableId, [
        //    'format' => 'rfc',
        //]), 'imported data comparison');

        // import data using incremental load
        $incLoadInputFile = __DIR__ . '/../../_data/tw_accounts.increment.csv';
        $incLoadExpectationFile = __DIR__ . '/../../_data/tw_accounts.expectation.increment.csv';
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($incLoadInputFile),
            [
                'incremental' => true,
            ]
        );

        //$this->assertLinesEqualsSorted(file_get_contents($incLoadExpectationFile), $this->_client->getTableDataPreview($tableId, [
        //    'format' => 'rfc',
        //]), 'imported data comparison');
    }

    public function testLoadTypedTables(): void
    {
        $fullLoadExpectationFile = __DIR__ . '/../../_data/tw_accounts.expectation.full.csv';
        $incLoadExpectationFile = __DIR__ . '/../../_data/tw_accounts.expectation.increment.csv';

        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $payload = [
            'name' => 'tw-accountsFull',
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

        // create table
        $tableId = $this->_client->createTableDefinition($bucketId, $payload);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        // import data
        $inputFile = __DIR__ . '/../../_data/tw_accounts.csv';
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($inputFile),
            [
                'incremental' => false,
            ]
        );

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'tw_loaded',
                    'columns' => [
                        ['source' => 'id', 'type' => 'INT'],
                        ['source' => 'idTwitter', 'type' => 'BIGINT'],
                        ['source' => 'name', 'type' => 'NVARCHAR'],
                        ['source' => 'import', 'type' => 'INT'],
                        ['source' => 'isImported', 'type' => 'TINYINT'],
                        ['source' => 'apiLimitExceededDatetime', 'type' => 'DATETIME2'],
                        ['source' => 'analyzeSentiment', 'type' => 'TINYINT'],
                        ['source' => 'importKloutScore', 'type' => 'INT'],
                        ['source' => 'timestamp', 'type' => 'DATETIME2'],
                        ['source' => 'oauthToken', 'type' => 'NVARCHAR'],
                        ['source' => 'oauthSecret', 'type' => 'NVARCHAR'],
                        ['source' => 'idApp', 'type' => 'INT'],
                    ],
                ],
            ],
        ];
        $expectedColumnsInWorkspace = [
            'id',
            'idTwitter',
            'name',
            'import',
            'isImported',
            'apiLimitExceededDatetime',
            'analyzeSentiment',
            'importKloutScore',
            'timestamp',
            'oauthToken',
            'oauthSecret',
            'idApp',
        ];

        $expectedTableTypesInWorkspace = [
            'INT',
            'BIGINT',
            'NVARCHAR',
            'INT',
            'TINYINT',
            'DATETIME2',
            'TINYINT',
            'INT',
            'DATETIME2',
            'NVARCHAR',
            'NVARCHAR',
            'INT',
        ];

        // load table to WS
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $ref = $backend->getTableReflection('tw_loaded');
        self::assertSame($expectedColumnsInWorkspace, $ref->getColumnsNames());
        self::assertSame(3, $ref->getRowsCount());
        $this->assertArrayEqualsSorted(
            $this->parseCsv($fullLoadExpectationFile, true),
            $backend->fetchAll('tw_loaded', \PDO::FETCH_ASSOC),
            'id',
            'imported data comparison'
        );
        $types = array_map(function ($columnDefinition) {
            /** @var $columnDefinition SynapseColumn */
            return $columnDefinition->getColumnDefinition()->getType();
        }, iterator_to_array($ref->getColumnsDefinitions()));
        self::assertSame($expectedTableTypesInWorkspace, $types);

        // import empty csv (truncate table)
        $inputFile = __DIR__ . '/../../_data/tw_accounts.empty.csv';
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($inputFile),
            [
                'incremental' => false,
            ]
        );
        // write data from WS back to storage
        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataObject' => 'tw_loaded',
            'incremental' => false,
            'columns' => $expectedColumnsInWorkspace,
        ]);

        // preview will convert nulls to "" it can't be compered
        //$this->assertLinesEqualsSorted(
        //    file_get_contents($fullLoadExpectationFile),
        //    $this->_client->getTableDataPreview($tableId, [
        //        'format' => 'rfc',
        //    ]),
        //    'imported data comparison'
        //);

        // import incremental data with full load to storage
        $inputFile = __DIR__ . '/../../_data/tw_accounts.increment.csv';
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($inputFile),
            [
                'incremental' => false,
            ]
        );
        // load table to WS incrementally
        $options['input'][0]['incremental'] = true;
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        self::assertSame($expectedColumnsInWorkspace, $ref->getColumnsNames());
        self::assertSame(4, $ref->getRowsCount());
        $this->assertArrayEqualsSorted(
            $this->parseCsv($incLoadExpectationFile, true),
            $backend->fetchAll('tw_loaded', \PDO::FETCH_ASSOC),
            'id',
            'imported data comparison'
        );

        // write data back to storage incrementally
        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataObject' => 'tw_loaded',
            'incremental' => true,
            'columns' => $expectedColumnsInWorkspace,
        ]);

        // preview will convert nulls to "" it can't be compered
        //$incLoadExpectationFile = __DIR__ . '/../../_data/tw_accounts.expectation.increment.csv';
        //$this->assertLinesEqualsSorted(
        //    file_get_contents($incLoadExpectationFile),
        //    $this->_client->getTableDataPreview($tableId, [
        //        'format' => 'rfc',
        //    ]),
        //    'imported data comparison'
        //);
    }

    public function testLoadTypedTablesConversionError(): void
    {
        $fullLoadFile = __DIR__ . '/../../_data/users.csv';

        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $payload = [
            'name' => 'users-types',
            'primaryKeysNames' => ['id'],
            'columns' => [
                ['name' => 'id', 'definition' => ['type' => 'INT']],
                ['name' => 'name', 'definition' => ['type' => 'NVARCHAR']],
                ['name' => 'city', 'definition' => ['type' => 'NVARCHAR']],
                ['name' => 'sex', 'definition' => ['type' => 'INT']],
            ],
            'distribution' => [
                'type' => 'HASH',
                'distributionColumnsNames' => ['id'],
            ],
        ];
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        // create table
        $tableId = $this->_client->createTableDefinition($bucketId, $payload);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        try {
            // try import data with wrong types with full load
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($fullLoadFile),
                [
                    'incremental' => false,
                ]
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertSame('[SQL Server]Bulk load data conversion error (type mismatch or invalid character for the specified codepage) for row starting at byte offset 25, column 4 (sex) in data file /users.csv.gz.', $e->getMessage());
        }

        try {
            // try import data with wrong types with full load
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($fullLoadFile),
                [
                    'incremental' => true,
                ]
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertSame('[SQL Server]Bulk load data conversion error (type mismatch or invalid character for the specified codepage) for row starting at byte offset 25, column 4 (sex) in data file /users.csv.gz.', $e->getMessage());
        }
    }
}
