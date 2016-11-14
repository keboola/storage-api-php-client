<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 5/25/13
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;

class MaintenanceTest extends StorageApiTestCase
{

    public function testMaintenance()
    {
        try {
            $client = new \Keboola\StorageApi\Client(array(
                'token' => STORAGE_API_TOKEN,
                'url' => STORAGE_API_MAINTENANCE_URL,
                'backoffMaxTries' => 2,
            ));
            $client->verifyToken();
            $this->fail('maintenance exception should be thrown');
        } catch (\Keboola\StorageApi\MaintenanceException $e) {
            $this->assertNotEmpty($e->getRetryAfter());
            $this->assertEquals('MAINTENANCE', $e->getStringCode());
            $this->assertEquals(503, $e->getCode());
            $params = $e->getContextParams();
            $this->assertEquals('maintenance', $params["status"]);
            $this->assertArrayHasKey('reason', $params);
            $this->assertArrayHasKey('estimatedEndTime', $params);
        }
    }
}
