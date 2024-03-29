<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class JobPollDelayTest extends StorageApiTestCase
{

    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testAlternateJobPollDelay(): void
    {
        $methodUsed = false;

        $linearDelay = function ($tries) use (&$methodUsed) {
            $methodUsed = true;
            return (int) $tries;
        };

        $client = $this->getClient([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'jobPollRetryDelay' => $linearDelay,
        ]);

        $csvFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
        $tableId = $client->createTableAsync($this->getTestBucketId(), 'languages', $csvFile);

        $this->assertTrue($methodUsed);
    }

    public function testInvalidJobPollDelay(): void
    {
        $this->expectException(\TypeError::class);
        $dumbDelay = 'wait for 30 seconds';

        $client = $this->getClient([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'jobPollRetryDelay' => $dumbDelay,
        ]);
    }
}
