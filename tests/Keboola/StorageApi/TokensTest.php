<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

class Keboola_StorageApi_Buckets_TokensTest extends StorageApiTestCase
{

	protected $_inBucketId;
	protected $_outBucketId;


	public function setUp()
	{
		parent::setUp();

		$this->_outBucketId = $this->_initEmptyBucket('api-tests', 'out');
		$this->_inBucketId = $this->_initEmptyBucket('api-tests', 'in');

		$this->_initTokens();
	}

	protected function _initTokens()
	{
		foreach ($this->_client->listTokens() as $token) {
			if ($token['isMasterToken']) {
				continue;
			}
			$this->_client->dropToken($token['id']);
		}
	}

	public function testTokenLogData()
	{
		$this->_client->verifyToken();
		$logData = $this->_client->getLogData();
		$this->assertNotEmpty($logData);
	}

	public function testTokenManagement()
	{
		$description = 'Out read token';
		$bucketPermissions = array(
			$this->_outBucketId => 'read',
		);

		$tokenId = $this->_client->createToken($bucketPermissions, $description);

		// check created token
		$token = $this->_client->getToken($tokenId);
		$this->assertEquals($description, $token['description']);
		$this->assertFalse($token['canManageTokens']);
		$this->assertFalse($token['canManageBuckets']);
		$this->assertEquals($bucketPermissions, $token['bucketPermissions']);

		$tokens = $this->_client->listTokens();
		$this->assertCount(2, $tokens);

		// update and check token again
		$newBucketPermissions = array(
			$this->_inBucketId => 'write',
		);
		$this->_client->updateToken($tokenId, $newBucketPermissions);
		$token = $this->_client->getToken($tokenId);
		$this->assertEquals($newBucketPermissions, $token['bucketPermissions']);

		// drop token test
		$this->_client->dropToken($tokenId);
		$tokens = $this->_client->listTokens();
		$this->assertCount(1, $tokens);
	}

	public function testTokenRefresh()
	{
		$description = 'Out read token';
		$bucketPermissions = array(
			'out.c-api-tests' => 'read'
		);
		$tokenId = $this->_client->createToken($bucketPermissions, $description);
		$token = $this->_client->getToken($tokenId);

		$this->_client->refreshToken($tokenId);
		$tokenAfterRefresh = $this->_client->getToken($tokenId);

		$this->assertNotEquals($token['token'], $tokenAfterRefresh['token']);
	}

	public function testTokenPermissions()
	{
		// prepare token and test tables
		$inTableId = $this->_client->createTable($this->_inBucketId, 'languages', __DIR__ . '/_data/languages.csv');
		$outTableId = $this->_client->createTable($this->_outBucketId, 'languages', __DIR__ . '/_data/languages.csv');

		$description = 'Out read token';
		$bucketPermissions = array(
			'out.c-api-tests' => 'read'
		);
		$tokenId = $this->_client->createToken($bucketPermissions, $description);
		$token = $this->_client->getToken($tokenId);

		$client = new Keboola\StorageApi\Client($token['token'], STORAGE_API_URL);

		// token getter
		$this->assertEquals($client->getTokenString(), $token['token']);
		$this->assertEmpty($token['expires']);
		$this->assertFalse($token['isExpired']);

		// check assigned buckets
		$buckets = $client->listBuckets();
		$this->assertCount(1, $buckets);
		$bucket = reset($buckets);
		$this->assertEquals('out.c-api-tests', $bucket['id']);

		// check assigned tables
		$tables = $client->listTables();
		$this->assertCount(1, $tables);
		$table = reset($tables);
		$this->assertEquals($outTableId, $table['id']);

		// read from table
		$tableData = $client->exportTable($outTableId);
		$this->assertNotEmpty($tableData);

		try {
			$client->exportTable($inTableId);
			$this->fail('Table exported with no permissions');
		} catch (Keboola\StorageApi\ClientException $e) {}

		// write into table
		try {
			$client->writeTable($outTableId, __DIR__ . '/_data/languages.csv');
			$this->fail('Table imported with read token');
		} catch (Keboola\StorageApi\ClientException  $e) {}

		try {
			$client->writeTable($inTableId, __DIR__ . '/_data/languages.csv');
			$this->fail('Table imported with no permissions');
		} catch (Keboola\StorageApi\ClientException  $e) {}

	}

	public function testAllBucketsTokenPermissions()
	{
		$description = 'Out read token';
		$tokenId = $this->_client->createToken('manage', $description);
		$token = $this->_client->getToken($tokenId);

		$client = new Keboola\StorageApi\Client($token['token'], STORAGE_API_URL);

		// token getter
		$this->assertEquals($client->getTokenString(), $token['token']);
		$this->assertEmpty($token['expires']);
		$this->assertFalse($token['isExpired']);

		// check assigned buckets
		$buckets = $client->listBuckets();
		$this->assertCount(4, $buckets);

		// create new bucket with master token
		$newBucketId = $this->_client->createBucket('test', 'in', 'testing');

		// check if new token has access to token
		$buckets = $client->listBuckets();
		$this->assertCount(5, $buckets);

		$bucket = $client->getBucket($newBucketId);
		$client->dropBucket($newBucketId);

	}

	public function testTokenWithExpiration()
	{
		$description = 'Out read token with expiration';
		$bucketPermissions = array(
			'out.c-api-tests' => 'read'
		);
		$twoMinutesExpiration = 2 * 60;
		$tokenId = $this->_client->createToken($bucketPermissions, $description, $twoMinutesExpiration);
		$token = $this->_client->getToken($tokenId);

		$client = new Keboola\StorageApi\Client($token['token'], STORAGE_API_URL);
		$token = $client->verifyToken();
		$this->assertNotEmpty($token['expires']);
		$this->assertFalse($token['isExpired']);
	}

	public function testExpiredToken()
	{
		$description = 'Out read token with expiration';
		$bucketPermissions = array(
			'out.c-api-tests' => 'read'
		);
		$oneSecondExpiration = 1;
		$tokenId = $this->_client->createToken($bucketPermissions, $description, $oneSecondExpiration);
		$token = $this->_client->getToken($tokenId);
		sleep(2);

		$client = null;
		try {
			$client = new Keboola\StorageApi\Client($token['token'], STORAGE_API_URL);
		} catch(\Keboola\StorageApi\ClientException $e) {
			if ($e->getStringCode() !== 'storage.tokenExpired') {
				$this->fail('storage.tokenExpired code should be rerturned from API.');
			}
		}
		if ($client !== null) {
			$this->fail('It should not be able to login with expired token');
		}

		$tokens = $this->_client->listTokens();
		$this->assertCount(2, $tokens);

		$token = $this->_client->getToken($tokenId);
		$this->assertTrue($token['isExpired']);
	}
}