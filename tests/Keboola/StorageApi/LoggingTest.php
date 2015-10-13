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
		$this->assertArrayHasKey('admin', $logData);

		$admin = $logData['admin'];
		$this->assertInternalType('int', $admin['id']);
		$this->assertNotEmpty($admin['name']);
	}

	public function testLogger()
	{
		$logger = $this->getMockBuilder('\Psr\Log\NullLogger')
			->getMock();

		$logger->expects($this->once())
			->method('log');

		$client = new \Keboola\StorageApi\Client(array(
			'token' => STORAGE_API_TOKEN,
			'url' => STORAGE_API_URL,
			'logger' => $logger,
		));
		$client->verifyToken();
	}

}