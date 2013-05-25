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

	public function testInvalidToken()
	{
		$invalidToken = 'tohlejeneplatnytoken';

		try {
			$client = new \Keboola\StorageApi\Client($invalidToken, STORAGE_API_URL);
			$this->fail('Exception should be thrown on invalid token');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertNotContains($invalidToken, $e->getMessage(), "Token value should not be returned back");

		}
	}

	public function testTokenManagement()
	{
		$initialTokens = $this->_client->listTokens();
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
		$this->assertCount(count($initialTokens) + 1, $tokens);

		// update and check token again
		$newBucketPermissions = array(
			$this->_inBucketId => 'write',
		);
		$this->_client->updateToken($tokenId, $newBucketPermissions);
		$token = $this->_client->getToken($tokenId);
		$this->assertEquals($newBucketPermissions, $token['bucketPermissions']);

		// invalid permission
		$invalidBucketPermissions = array(
			$this->_inBucketId => 'manage',
		);
		try {
			$this->_client->updateToken($tokenId, $invalidBucketPermissions);
			$this->fail('Manage permissions shouild not be allower to set');
		} catch(\Keboola\StorageApi\ClientException $e) {}
		$token = $this->_client->getToken($tokenId);
		$this->assertEquals($newBucketPermissions, $token['bucketPermissions']);

		// drop token test
		$this->_client->dropToken($tokenId);
		$tokens = $this->_client->listTokens();
		$this->assertCount(count($initialTokens), $tokens);
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
		$inTableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile(__DIR__ . '/_data/languages.csv'));
		$outTableId = $this->_client->createTable($this->_outBucketId, 'languages', new CsvFile(__DIR__ . '/_data/languages.csv'));

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
			$client->writeTable($outTableId, new CsvFile(__DIR__ . '/_data/languages.csv'));
			$this->fail('Table imported with read token');
		} catch (Keboola\StorageApi\ClientException  $e) {}

		try {
			$client->writeTable($inTableId, new CsvFile(__DIR__ . '/_data/languages.csv'));
			$this->fail('Table imported with no permissions');
		} catch (Keboola\StorageApi\ClientException  $e) {}

	}

	public function testAssignNonExistingBucketShouldFail()
	{
		$bucketPermissions = array(
			'out.tohle-je-hodne-blby-nazev' => 'read'
		);

		try {
			$this->_client->createToken($bucketPermissions, 'Some description');
			$this->fail('Invalid permissions exception should be thrown');
		} catch(\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
		}

	}

	public function testAllBucketsTokenPermissions()
	{
		$description = 'Out read token';
		$bucketsInitialCount = count($this->_client->listBuckets());
		$tokenId = $this->_client->createToken('manage', $description);
		$token = $this->_client->getToken($tokenId);

		$client = new Keboola\StorageApi\Client($token['token'], STORAGE_API_URL);

		// token getter
		$this->assertEquals($client->getTokenString(), $token['token']);
		$this->assertEmpty($token['expires']);
		$this->assertFalse($token['isExpired']);

		// check assigned buckets
		$buckets = $client->listBuckets();
		$this->assertCount($bucketsInitialCount, $buckets);

		// create new bucket with master token
		$newBucketId = $this->_client->createBucket('test', 'in', 'testing');

		// check if new token has access to token
		$buckets = $client->listBuckets();
		$this->assertCount($bucketsInitialCount + 1, $buckets);

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
		$initialTokens = $this->_client->listTokens();

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
		$this->assertCount(count($initialTokens) + 1, $tokens);

		$token = $this->_client->getToken($tokenId);
		$this->assertTrue($token['isExpired']);
	}

}