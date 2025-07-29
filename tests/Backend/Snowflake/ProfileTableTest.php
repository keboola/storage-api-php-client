<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Snowflake;

use DateTime;
use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Test\StorageApiTestCase;

final class ProfileTableTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();

        $this->allowTestForBackendsOnly(
            [StorageApiTestCase::BACKEND_SNOWFLAKE],
            'Data profiling is now available only for Snowflake.',
        );

        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableProfile(): void
    {
        $started = new DateTime();

        $tableDefinition = [
            'name' => 'products',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => Snowflake::TYPE_INT,
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'col_string',
                    'definition' => [
                        'type' => Snowflake::TYPE_STRING,
                        'nullable' => true,
                    ],
                ],
                [
                    'name' => 'col_bool',
                    'definition' => [
                        'type' => Snowflake::TYPE_BOOLEAN,
                        'nullable' => true,
                    ],
                ],
                [
                    'name' => 'col_int',
                    'definition' => [
                        'type' => Snowflake::TYPE_INT,
                        'nullable' => true,
                    ],
                ],
                [
                    'name' => 'col_decimal',
                    'definition' => [
                        'type' => Snowflake::TYPE_DECIMAL,
                        'nullable' => true,
                    ],
                ],
                [
                    'name' => 'col_float',
                    'definition' => [
                        'type' => Snowflake::TYPE_FLOAT,
                        'nullable' => true,
                    ],
                ],
                [
                    'name' => 'col_date',
                    'definition' => [
                        'type' => Snowflake::TYPE_DATE,
                        'nullable' => true,
                    ],
                ],
            ],
        ];

        $tableId = $this->_client->createTableDefinition($this->getTestBucketId(), $tableDefinition);

        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/profiling.csv'),
        );

        $job = $this->_client->profileTable($tableId);
        $profile = $this->_client->getTableProfile($tableId);

        $this->assertEquals($job, $profile);

        $keys = array_keys($profile);
        $this->assertSame(
            ['uuid', 'bucketId', 'tableName', 'createdAt', 'profile', 'columns', 'profiledByToken'],
            $keys,
        );

        $this->assertSame($tableId, $profile['tableName']);

        $createdAt = DateTime::createFromFormat('Y-m-d H:i:s', $profile['createdAt']);
        $this->assertTrue($createdAt > $started);

        $tableProfile = [
            'dataSize' => 3072,
            'rowCount' => 8,
            'columnCount' => 8,
        ];
        $this->assertSame($tableProfile, $profile['profile']);

        $columnProfiles = [
            [
                'name' => 'id',
                'profile' => [
                    'nullCount' => 0,
                    'distinctCount' => 8,
                    'duplicateCount' => 0,
                ],
            ],
            [
                'name' => 'col_string',
                'profile' => [
                    'nullCount' => 0,
                    'distinctCount' => 7,
                    'duplicateCount' => 1,
                ],
            ],
            [
                'name' => 'col_bool',
                'profile' => [
                    'nullCount' => 1,
                    'distinctCount' => 2,
                    'duplicateCount' => 5,
                ],
            ],
            [
                'name' => 'col_int',
                'profile' => [
                    'nullCount' => 1,
                    'distinctCount' => 5,
                    'duplicateCount' => 2,
                ],
            ],
            [
                'name' => 'col_decimal',
                'profile' => [
                    'nullCount' => 1,
                    'distinctCount' => 6,
                    'duplicateCount' => 1,
                ],
            ],
            [
                'name' => 'col_float',
                'profile' => [
                    'nullCount' => 1,
                    'distinctCount' => 5,
                    'duplicateCount' => 2,
                ],
            ],
            [
                'name' => 'col_date',
                'profile' => [
                    'nullCount' => 1,
                    'distinctCount' => 6,
                    'duplicateCount' => 1,
                ],
            ],
        ];
        $this->assertSame($columnProfiles, $profile['columns']);

        $token = $this->_client->verifyToken();
        $this->assertSame(
            [
                'id' => (int) $token['id'],
                'name' => $token['description'],
            ],
            $profile['profiledByToken'],
        );
    }
}
