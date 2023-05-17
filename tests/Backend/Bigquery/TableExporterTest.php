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
     * @dataProvider  filterProvider
     */
    public function testColumnTypesInTableDefinition(array $params, string $expectExceptionMessage): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'my-new-table-for_data_preview',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'column_int',
                    'definition' => [
                        'type' => 'INT64',
                    ],
                ],
                [
                    'name' => 'column_number',
                    'definition' => [
                        'type' => 'NUMERIC',
                    ],
                ],
                [
                    'name' => 'column_float',
                    'definition' => [
                        'type' => 'FLOAT64',
                    ],
                ],
                [
                    'name' => 'column_varchar',
                    'definition' => [
                        'type' => 'STRING',
                    ],
                ],
                [
                    'name' => 'column_datetime',
                    'definition' => [
                        'type' => 'DATETIME',
                    ],
                ],
                [
                    'name' => 'column_date',
                    'definition' => [
                        'type' => 'DATE',
                    ],
                ],
                [
                    'name' => 'column_time',
                    'definition' => [
                        'type' => 'TIME',
                    ],
                ],
                [
                    'name' => 'column_timestamp',
                    'definition' => [
                        'type' => 'TIMESTAMP',
                    ],
                ],
                [
                    'name' => 'column_boolean',
                    'definition' => [
                        'type' => 'BOOL',
                    ],
                ],
            ],
        ];

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow([
            'column_int',
            'column_number',
            'column_float',
            'column_varchar',
            'column_datetime',
            'column_date',
            'column_time',
            'column_timestamp',
            'column_boolean',
        ]);
        $csvFile->writeRow(
            [
                '1',
                '3.14',
                '3.14',
                'roman',
                '1989-08-31 00:00:00.000',
                '1989-08-31',
                '12:00:00.000',
                '2023-04-18 12:34:56',
                0,
            ]
        );

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTableAsync($tableId, $csvFile);

        $exporter = new TableExporter($this->_client);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectExceptionMessage);
        $exporter->exportTable($tableId, $this->downloadPath, $params);
    }

    public function filterProvider(): Generator
    {
        foreach (['rfc'] as $format) {
            yield 'wrong int '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_int',
                            'operator' => 'eq',
                            'values' => ['1'],
                        ],
                    ],
                ],
                'Invalid filter value, expected:"INT64", actual:"STRING".',
            ];

            yield 'wrong number '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_number',
                            'operator' => 'eq',
                            'values' => ['aaa'],
                        ],
                    ],
                ],
                'Invalid filter value, expected:"NUMERIC", actual:"STRING".',
            ];

            yield 'wrong float '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_float',
                            'operator' => 'eq',
                            'values' => ['aaa'],
                        ],
                    ],
                ],
                'Invalid filter value, expected:"FLOAT64", actual:"STRING".',
            ];

            yield 'wrong datetime '. $format => [
                [
                    'format' => $format,
                    'columns' => ['column_int', 'column_datetime'],
                    'whereFilters' => [
                        [
                            'column' => 'column_datetime',
                            'operator' => 'eq',
                            'values' => ['2022-02-31'],
                        ],
                    ],
                ],
                'Invalid datetime string "2022-02-31"; while executing the filter on column \'column_datetime\'; Column \'column_int\'', // non-existing date
            ];

            yield 'wrong boolean '. $format => [
                [
                    'whereFilters' => [
                        [
                            'column' => 'column_boolean',
                            'operator' => 'eq',
                            'values' => ['222'],
                        ],
                    ],
                ],
                'Invalid filter value, expected:"BOOL", actual:"STRING".',
            ];

            yield 'wrong date '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_date',
                            'operator' => 'eq',
                            'values' => ['12:00:00.000'],
                        ],
                    ],
                ],
                'Invalid date: \'12:00:00.000\'; while executing the filter on column \'column_date\'; Column \'column_int\'',
            ];

            yield 'wrong time '. $format => [
                [
                    'whereFilters' => [
                        [
                            'column' => 'column_time',
                            'operator' => 'eq',
                            'values' => ['1989-08-31'],
                        ],
                    ],
                ],
                'Invalid time string "1989-08-31"; while executing the filter on column \'column_time\'; Column \'column_int\'',
            ];

            yield 'wrong timestamp '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_timestamp',
                            'operator' => 'eq',
                            'values' => ['xxx'],
                        ],
                    ],
                ],
                'Invalid timestamp: \'xxx\'; while executing the filter on column \'column_timestamp\'; Column \'column_int\'',
            ];
        }
    }
}
