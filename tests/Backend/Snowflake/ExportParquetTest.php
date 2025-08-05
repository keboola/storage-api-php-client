<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\Backend\Bigquery\TestExportDataProvidersTrait;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\ParquetUtils;

class ExportParquetTest extends StorageApiTestCase
{
    use TestExportDataProvidersTrait;
    use ParquetUtils;

    private string $downloadPath;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
        $this->downloadPath = $this->getExportFilePathForTest('languages.sliced.parquet');
    }

    /**
     * @dataProvider tableExportData
     */
    public function testTableAsyncExport(CsvFile $importFile, string $expectationsFileName, array $exportOptions = [], string|int $orderBy = 'id'): void
    {
        $expectationsFile = __DIR__ . '/../../_data/bigquery/' . $expectationsFileName;

        if (!isset($exportOptions['gzip'])) {
            $exportOptions['gzip'] = false;
        }

        $exportOptions['fileType'] = 'parquet';

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

        $exporter->exportTable($tableId, $this->downloadPath, $exportOptions);

        // compare data
        $files = glob($this->downloadPath . '/*');
        $this->assertNotEmpty($files, 'No files found in download path: ' . $this->downloadPath);

        $content = $this->getParquetContent($files);

        $this->assertArrayEqualsSorted(
            Client::parseCsv((string) file_get_contents($expectationsFile), true),
            $content,
            $orderBy,
            'imported data comparison',
        );

        // check that columns has been set in export job params
        $jobs = $this->listJobsByRunId($runId);
        $job = reset($jobs);

        $this->assertSame($runId, $job['runId']);
        $this->assertSame('tableExport', $job['operationName']);
        $this->assertSame($tableId, $job['tableId']);
        $this->assertNotEmpty($job['operationParams']['export']['columns']);
        $this->assertSame($expectedColumns, $job['operationParams']['export']['columns']);
        $this->assertTrue($job['operationParams']['export']['gzipOutput']);
        $this->assertSame(2, $job['operationParams']['export']['fileType']);
    }

    // bigquery exports data different, so we create new files for BQ
    public function tableExportData(): array
    {
        $filesBasePath = __DIR__ . '/../../_data/bigquery/';
        return [
            [new CsvFile($filesBasePath . '1200.csv'), '1200.csv', [], 'col_1'],
            [new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv'],
            [new CsvFile($filesBasePath . 'languages.encoding.csv'), 'languages.encoding.csv'],
            [new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv', ['gzip' => true]],

            [new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv'],
            [new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv',  ['gzip' => true]],
            [new CsvFile($filesBasePath . '64K.csv'), '64K.csv'],
            [new CsvFile($filesBasePath . '64K.csv'), '64K.csv',  ['gzip' => true]],

            [new CsvFile($filesBasePath . 'escaping.csv'), 'escaping.standard.out.csv', ['gzip' => true], 'col1'],
        ];
    }
}
