<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class JobPollDelayTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testAlternateJobPollDelay()
    {
        $methodUsed = false;

        $linearDelay = function ($tries) use (&$methodUsed) {
            $methodUsed = true;
            return (int) $tries;
        };

        $client = $this->getClient(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'jobPollRetryDelay' => $linearDelay
        ));

        $csvFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
        $tableId = $client->createTableAsync($this->getTestBucketId(), 'languages', $csvFile);

        $this->assertTrue($methodUsed);
    }

    /**
     * @expectedException \TypeError
     */
    public function testInvalidJobPollDelay()
    {
        $dumbDelay = 'wait for 30 seconds';

        $client = $this->getClient(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'jobPollRetryDelay' => $dumbDelay
        ));
    }
}
