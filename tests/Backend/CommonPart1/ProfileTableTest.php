<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\CommonPart1;

use DateTime;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

final class ProfileTableTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();

        $this->allowTestForBackendsOnly(
            [StorageApiTestCase::BACKEND_BIGQUERY],
            'Data profiling is now available only for BigQuery.',
        );

        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableProfile(): void
    {
        $started = new DateTime();

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));

        $job = $this->_client->profileTable($tableId);
        $profile = $this->_client->getTableProfile($tableId);

        $this->assertEquals($job, $profile);

        $keys = array_keys($profile);
        $this->assertSame(['uuid', 'bucketId', 'tableName', 'createdAt', 'profile', 'columns'], $keys);

        $this->assertSame($tableId, $profile['tableName']);

        $createdAt = DateTime::createFromFormat('Y-m-d H:i:s', $profile['createdAt']);
        $this->assertTrue($createdAt > $started);

        $tableProfile = ['columns' => 2];
        $this->assertSame($tableProfile, $profile['profile']);

        $columnProfiles = [
            [
                'name' => 'id',
                'profile' => [
                    'average' => 42,
                ],
            ],
            [
                'name' => 'name',
                'profile' => [
                    'average' => 42,
                ],
            ],
        ];
        $this->assertSame($columnProfiles, $profile['columns']);
    }
}
