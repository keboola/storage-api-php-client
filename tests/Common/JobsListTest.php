<?php

declare(strict_types=1);

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class JobsListTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        // make 5 dummy jobs
        for ($i = 0; $i < 5; $i++) {
            $this->_client->createTableAsync(
                $this->getTestBucketId(),
                'dummy_job_'. $i,
                new CsvFile(__DIR__ . '/../_data/languages.csv')
            );
        }
    }

    public function testJobsListing(): void
    {
        $jobs = $this->_client->listJobs();

        // the default limit is 20 jobs
        $this->assertLessThanOrEqual(20, count($jobs));

        // get only 5 jobs
        $fiveJobs = $this->_client->listJobs(["limit" => 5]);
        $this->assertLessThanOrEqual(5, count($fiveJobs));

        // check the offset parameter
        $firstJob = $fiveJobs[0];
        $secondJob = $fiveJobs[1];
        $offsetJobs = $this->_client->listJobs(["limit" => 5, "offset" => 1]);
        $this->assertNotEquals($firstJob, $offsetJobs[0]);
        $this->assertEquals($secondJob, $offsetJobs[0]);
    }
}
