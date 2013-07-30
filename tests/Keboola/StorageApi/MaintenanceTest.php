<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 5/25/13
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */

class Keboola_StorageApi_MaintenanceTest extends StorageApiTestCase
{


	public function testMaintenance()
	{
		try {
			$client = new \Keboola\StorageApi\Client(STORAGE_API_TOKEN, STORAGE_API_MAINTENANCE_URL, '',  $backoffMaxTries = 0);
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