<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */


namespace Keboola\Test\Backend\Bigquery;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\ProcessPolyfill;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;

class TableExporterTest extends StorageApiTestCase
{
    use TestExportDataProvidersTrait;

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
    public function testTableAsyncExport(CsvFile $importFile, string $expectationsFileName, array $exportOptions = []): void
    {
        $expectationsFile = __DIR__ . '/../../_data/bigquery/' . $expectationsFileName;

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

    // bigquery exports data different, so we create new files for BQ
    public function tableExportData(): array
    {
        $filesBasePath = __DIR__ . '/../../_data/bigquery/';
        return [
            [new CsvFile($filesBasePath . '1200.csv'), '1200.csv'],
            [new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv'],
            [new CsvFile($filesBasePath . 'languages.encoding.csv'), 'languages.encoding.csv'],
            [new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv', ['gzip' => true]],

            [new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv'],
            [new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv',  ['gzip' => true]],
            [new CsvFile($filesBasePath . '64K.csv'), '64K.csv'],
            [new CsvFile($filesBasePath . '64K.csv'), '64K.csv',  ['gzip' => true]],

            [new CsvFile($filesBasePath . 'escaping.csv'), 'escaping.standard.out.csv', ['gzip' => true]],
        ];
    }

    /**
     * @dataProvider wrongDatatypeFilterProvider
     */
    public function testColumnTypesInTableDefinition(array $params, string $expectExceptionMessage): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableId = $this->_client->createTableDefinition($bucketId, $this->getTestTableDefinitions());

        $this->_client->writeTableAsync($tableId, $this->getTestCsv());

        $exporter = new TableExporter($this->_client);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectExceptionMessage);
        $exporter->exportTable($tableId, $this->downloadPath, $params);
    }

    public function wrongDatatypeFilterProvider(): Generator
    {
        return $this->getWrongDatatypeFilters(['rfc']);
    }
}
