<?php

namespace Keboola\Test\Backend\Bigquery;

use Generator;
use Keboola\Csv\CsvFile;

trait TestExportDataProvidersTrait
{
    public function getTestTableColumns(): array
    {
        return [
            'column_int',
            'column_number',
            'column_float',
            'column_varchar',
            'column_datetime',
            'column_date',
            'column_time',
            'column_timestamp',
            'column_boolean',
        ];
    }

    public function getTestCsv(): CsvFile
    {
        $csvFile = $this->createTempCsv();
        $csvFile->writeRow($this->getTestTableColumns());
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
            ],
        );

        return $csvFile;
    }

    public function getTestTableDefinitions(): array
    {
        return [
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
    }

    public function getWrongDatatypeFilters(array $formats): Generator
    {
        foreach ($formats as $format) {
            yield 'wrong int ' . $format => [
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
                1,
            ];

            yield 'wrong number ' . $format => [
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
            ];

            yield 'wrong float ' . $format => [
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
            ];

            yield 'wrong datetime ' . $format => [
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
            ];

            yield 'wrong boolean ' . $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_boolean',
                            'operator' => 'eq',
                            'values' => ['222'],
                        ],
                    ],
                ],
            ];

            yield 'wrong date ' . $format => [
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
            ];

            yield 'wrong time ' . $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_time',
                            'operator' => 'eq',
                            'values' => ['1989-08-31'],
                        ],
                    ],
                ],
            ];

            yield 'wrong timestamp ' . $format => [
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
            ];
        }
    }
}
