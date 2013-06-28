<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_Buckets_TokensTest extends StorageApiTestCase
{

	public function testMasterTokenShouldNotBeShareable()
	{
		try {
			$logData = $this->_client->getLogData();
			$this->_client->shareToken($logData['id'], 'test@devel.keboola.com', 'Hi');
			$this->fail('Master token should not be shareable');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.token.cannotShareMasterToken', $e->getStringCode());
		}
	}

	public function testTokenShare()
	{
		$newTokenId = $this->_client->createToken(array());
		$this->_client->shareToken($newTokenId, 'test@devel.keboola.com', 'Hi');
	}

}