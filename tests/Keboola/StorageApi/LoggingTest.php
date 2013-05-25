<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

class Keboola_StorageApi_LoggingTest extends StorageApiTestCase
{


	public function testLogData()
	{
		$this->_client->verifyToken();
		$logData = $this->_client->getLogData();
		$this->assertNotEmpty($logData);
		$this->assertInternalType('array', $logData);

		$this->assertArrayHasKey('id', $logData);
		$this->assertArrayHasKey('token', $logData);
		$this->assertArrayHasKey('owner', $logData);
	}

}