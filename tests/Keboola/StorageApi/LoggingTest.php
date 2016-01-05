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