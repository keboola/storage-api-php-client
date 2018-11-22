<?php

declare(strict_types=1);

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;

class JobsListTest extends StorageApiTestCase
{
    public function testJobsListing(): void
    {
        $jobs = $this->_client->listJobs();

        // the default limit is 20 jobs
        $this->assertLessThanOrEqual(20, count($jobs));

        // get only 5 jobs
        $fiveJobs = $this->_client->listJobs(["limit" => 5]);
        $this->assertLessThanOrEqual(5, count($fiveJobs));

        //if there are at least 2 jobs we can check the offset parameter
        if (count($fiveJobs) >= 2) {
            $firstJob = $fiveJobs[0];
            $secondJob = $fiveJobs[1];
            $offsetJobs = $this->_client->listJobs(["limit" => 5, "offset" => 1]);
            $this->assertNotEquals($firstJob, $offsetJobs[0]);
            $this->assertEquals($secondJob, $offsetJobs[0]);
        }
    }
}
