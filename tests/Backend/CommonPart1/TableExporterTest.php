<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */


namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\ProcessPolyfill;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\TableExporter;

class TableExporterTest extends StorageApiTestCase
{


    private string $downloadPath;
    private string $downloadPathGZip;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
        $this->downloadPath = $this->getExportFilePathForTest('languages.sliced.csv');
        $this->downloadPathGZip = $this->getExportFilePathForTest('languages.sliced.csv.gz');
    }

    /**
     * @dataProvider tableExportData
     */
    public function testTableAsyncExport(array $supportedBackends, CsvFile $importFile, $expectationsFileName, $exportOptions = []): void
    {
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;
        $tokenData = $this->_client->verifyToken();
        $defaultBackend = $tokenData['owner']['defaultBackend'];
        if (!in_array($defaultBackend, $supportedBackends)) {
            $this->markTestSkipped(sprintf(
                'Backend "%s" is not supported in this test case (%s are allowed)',
                $defaultBackend,
                implode(', ', $supportedBackends)
            ));
        }

        if (!isset($exportOptions['gzip'])) {
            $exportOptions['gzip'] = false;
        }

        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', $importFile);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $exporter = new TableExporter($this->_client);

        if (isset($exportOptions['columns'])) {
            $expectedColumns = $exportOptions['columns'];
        } else {
            $table = $this->_client->getTable($tableId);
            $expectedColumns = $table['columns'];
        }

        if ($exportOptions['gzip'] === true) {
            $exporter->exportTable($tableId, $this->downloadPathGZip, $exportOptions);
            if (file_exists($this->downloadPath)) {
                unlink($this->downloadPath);
            }
            $process = ProcessPolyfill::createProcess('gunzip ' . escapeshellarg($this->downloadPathGZip));
            if (0 !== $process->run()) {
                throw new \Symfony\Component\Process\Exception\ProcessFailedException($process);
            }
        } else {
            $exporter->exportTable($tableId, $this->downloadPath, $exportOptions);
        }

        // compare data
        $this->assertTrue(file_exists($this->downloadPath));
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($this->downloadPath), 'imported data comparison');

        // check that columns has been set in export job params
        $jobs = $this->listJobsByRunId($runId);
        $job = reset($jobs);

        $this->assertSame($runId, $job['runId']);
        $this->assertSame('tableExport', $job['operationName']);
        $this->assertSame($tableId, $job['tableId']);
        $this->assertNotEmpty($job['operationParams']['export']['columns']);
        $this->assertSame($expectedColumns, $job['operationParams']['export']['columns']);
        $this->assertTrue($job['operationParams']['export']['gzipOutput']);
    }

    public function testLimitParameter(): void
    {
        $this->skipTestForBackend([
            self::BACKEND_TERADATA,
            self::BACKEND_BIGQUERY
        ],'Not all preview params are supported');

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);
        $this->_client->writeTable($tableId, $importFile);

        $exportOptions = [
            'limit' => 2,
        ];
        $exporter = new TableExporter($this->_client);
        $exporter->exportTable($tableId, $this->downloadPath, $exportOptions);
        $this->assertTrue(file_exists($this->downloadPath));
        $parsed = Client::parseCsv(file_get_contents($this->downloadPath));
        $this->assertCount($exportOptions['limit'], $parsed);
    }


    public function testExportTablesEmptyColumns(): void
    {
        $filesBasePath = __DIR__ . '/../../_data/';
        $table1Id = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages1', new CsvFile($filesBasePath . 'languages.csv'));
        $table2Id = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages2', new CsvFile($filesBasePath . 'languages.csv'));

        $table1 = $this->_client->getTable($table1Id);
        $table2 = $this->_client->getTable($table2Id);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $exporter = new TableExporter($this->_client);
        $file1 = $this->getExportFilePathForTest('languages1.csv');
        $file2 = $this->getExportFilePathForTest('languages2.csv');
        $exports = [
            [
                'tableId' => $table1Id,
                'destination' => $file1,
            ],
            [
                'tableId' => $table2Id,
                'destination' => $file2,
                'columns' => [],
            ],

        ];
        $jobResults = $exporter->exportTables($exports);
        // compare data
        $this->assertTrue(file_exists($file1));
        $this->assertTrue(file_exists($file2));
        $expected = $filesBasePath . 'languages.csv';
        if ($this->getDefaultBackend($this->_client) === self::BACKEND_BIGQUERY) {
            // big query will strip " characters
            $expected = $filesBasePath . 'languages.expected.bigquery.csv';
        }
        $this->assertLinesEqualsSorted(file_get_contents($expected), file_get_contents($file1), 'imported data comparison');
        $this->assertLinesEqualsSorted(file_get_contents($expected), file_get_contents($file2), 'imported data comparison');

        // check that columns has been set in export job params
        $table1Job = null;
        $table2Job = null;

        $listedJobs = $this->listJobsByRunId($runId);
        $this->assertArrayEqualsSorted($listedJobs, $jobResults, 'id');
        foreach ($listedJobs as $job) {
            $this->assertSame($runId, $job['runId']);
            $this->assertSame('tableExport', $job['operationName']);

            if ($job['tableId'] === $table1Id) {
                $table1Job = $job;
            }
            if ($job['tableId'] === $table2Id) {
                $table2Job = $job;
            }
        }

        $this->assertNotEmpty($table1Job['operationParams']['export']['columns']);
        $this->assertSame($table1['columns'], $table1Job['operationParams']['export']['columns']);

        $this->assertNotEmpty($table2Job['operationParams']['export']['columns']);
        $this->assertSame($table2['columns'], $table2Job['operationParams']['export']['columns']);
    }

    public function testExportTablesWithColumns(): void
    {
        $filesBasePath = __DIR__ . '/../../_data/';
        $table1Id = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages1', new CsvFile($filesBasePath . 'languages.csv'));

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $exporter = new TableExporter($this->_client);
        $file1 = $this->getExportFilePathForTest('languages1.csv');
        $exports = [
            [
                'tableId' => $table1Id,
                'destination' => $file1,
                'exportOptions' => [
                    'whereFilters' => [
                        [
                            'column' => 'id',
                            'values' => [
                                '0',
                            ],
                        ],
                    ],
                    'columns' => ['name', 'id'],
                ],
            ],
        ];
        $exporter->exportTables($exports);

        // compare data
        $this->assertTrue(file_exists($file1));

        $csvFile = new \Keboola\Csv\CsvFile($file1);
        $this->assertEquals(['name', 'id'], $csvFile->getHeader());

        $csvFile->next();
        $this->assertEquals(['- unchecked -', '0'], $csvFile->current());

        // check that columns has been set in export job params
        $jobs = $this->listJobsByRunId($runId);
        $table1Job = reset($jobs);

        $this->assertSame($runId, $table1Job['runId']);
        $this->assertSame('tableExport', $table1Job['operationName']);
        $this->assertSame($table1Id, $table1Job['tableId']);
        $this->assertNotEmpty($table1Job['operationParams']['export']['columns']);
        $this->assertSame(['name', 'id'], $table1Job['operationParams']['export']['columns']);
    }

    public function testExportTablesMissingTableId(): void
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables([[]]);
            $this->fail('Missing exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing tableId', $e->getMessage());
        }
    }

    public function testExportTablesEmptyTableId(): void
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables([['tableId' => '']]);
            $this->fail('Missing exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing tableId', $e->getMessage());
        }
    }

    public function testExportTablesMissingDestination(): void
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables([['tableId' => 'dummy']]);
            $this->fail('Missing exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing destination', $e->getMessage());
        }
    }

    public function testExportTablesEmptyDestination(): void
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables([['tableId' => 'dummy', 'destination' => '']]);
            $this->fail('Missing exception');
        } catch (Exception $e) {
            $this->assertEquals('Missing destination', $e->getMessage());
        }
    }

    public function testExportTablesInvalidTable(): void
    {
        $exporter = new TableExporter($this->_client);
        try {
            $exporter->exportTables([['tableId' => 'in.c-dummy.dummy', 'destination' => 'dummy']]);
            $this->fail('Missing exception');
        } catch (ClientException $e) {
            $this->assertStringContainsString('The table "dummy" was not found in the bucket "in.c-dummy"', $e->getMessage());
        }
    }

    public function tableExportData(): array
    {
        $filesBasePath = __DIR__ . '/../../_data/';
        return [
            [[self::BACKEND_SNOWFLAKE, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . '1200.csv'), '1200.csv'],
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_SYNAPSE, self::BACKEND_EXASOL, self::BACKEND_TERADATA, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv'],
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_SYNAPSE, self::BACKEND_EXASOL, self::BACKEND_TERADATA, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . 'languages.encoding.csv'), 'languages.encoding.csv'],
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_SYNAPSE, self::BACKEND_EXASOL, self::BACKEND_TERADATA, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv', ['gzip' => true]],
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_SYNAPSE, self::BACKEND_EXASOL, self::BACKEND_TERADATA], new CsvFile($filesBasePath . 'numbers.csv'), 'numbers.csv'],
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_SYNAPSE, self::BACKEND_EXASOL], new CsvFile($filesBasePath . 'numbers.csv'), 'numbers.two-cols.csv', ['columns' => ['0', '45']]],
            // TODO add TD ^^

            // tests the redshift data too long bug https://github.com/keboola/connection/issues/412
            // TD skipped because of TD limit 10666 chars
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_EXASOL, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv'],
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_EXASOL, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv',  ['gzip' => true]],
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_EXASOL, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . '64K.csv'), '64K.csv'],
            [[self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, self::BACKEND_EXASOL, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . '64K.csv'), '64K.csv',  ['gzip' => true]],

            [[self::BACKEND_REDSHIFT, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . 'escaping.csv'), 'escaping.standard.out.csv', ['gzip' => true]],
            [[self::BACKEND_REDSHIFT, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . 'numbers.csv'), 'numbers.csv', ['gzip' => true]],
            [[self::BACKEND_REDSHIFT, self::BACKEND_BIGQUERY], new CsvFile($filesBasePath . 'numbers.csv'), 'numbers.two-cols.csv', ['gzip' => true, 'columns' => ['0', '45']]],
        ];
    }
}
