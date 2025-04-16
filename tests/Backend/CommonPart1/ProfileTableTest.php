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
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableProfile(): void
    {
        $started = new DateTime();

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));

        $this->_client->profileTable($tableId);

        $profile = $this->_client->getTableProfile($tableId);

        $keys = array_keys($profile);
        $this->assertEquals(['uuid', 'bucketId', 'tableName', 'createdAt', 'profile', 'columns'], $keys);

        $this->assertEquals($tableId, $profile['tableName']);

        $createdAt = DateTime::createFromFormat('Y-m-d H:i:s', $profile['createdAt']);
        $this->assertTrue($createdAt > $started);

        $tableProfile = ['columns' => 2];
        $this->assertEquals($tableProfile, $profile['profile']);

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
        $this->assertEquals($columnProfiles, $profile['columns']);
    }
}
