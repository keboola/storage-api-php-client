<?php
namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Client;

class TokensTest extends StorageApiTestCase
{
    /** @var string */
    private $inBucketId;

    /** @var string */
    private $outBucketId;

    public function setUp()
    {
        parent::setUp();

        $this->_initEmptyTestBuckets();

        $this->outBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $this->inBucketId = $this->getTestBucketId(self::STAGE_IN);

        $this->initTokens();
    }

    private function initTokens()
    {
        foreach ($this->_client->listTokens() as $token) {
            if ($token['isMasterToken']) {
                continue;
            }

            $this->_client->dropToken($token['id']);
        }
    }

    public function testTokenRefresh()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $tokenString = $token['token'];
        $created = new \DateTime($token['created']);

        sleep(1);

        $this->_client->refreshToken($tokenId);
        $token = $this->_client->getToken($tokenId);

        $refreshed = new \DateTime($token['refreshed']);

        $this->assertNotEquals($tokenString, $token['token']);
        $this->assertGreaterThan($created->getTimestamp(), $refreshed->getTimestamp());
    }

    public function testCreateTokenWithoutDescriptionGetsDefautGeneratedDescription()
    {
        $currentToken = $this->_client->verifyToken();

        $tokenId = $this->_client->createToken(new TokenCreateOptions());
        $token = $this->_client->getToken($tokenId);

        $this->assertEquals('Created by ' . $currentToken['description'], $token['description']);
    }

    public function testTokenWithExpiration()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token with expiration')
            ->setExpiresIn(2 * 60)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        $token = $client->verifyToken();

        $this->assertNotEmpty($token['expires']);
        $this->assertFalse($token['isExpired']);
    }

    public function testExpiredToken()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Out read token with expiration')
            ->setExpiresIn(1)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);
        $tries = 0;

        $this->expectException(ClientException::class);
        while ($tries < 7) {
            $client = new Client([
                'token' => $token['token'],
                'url' => STORAGE_API_URL,
            ]);
            $client->verifyToken();
            sleep(pow(2, $tries++));
        }

        $this->fail('token should be invalid');
    }

    public function testAllBucketsTokenPermissions()
    {
        $bucketsInitialCount = count($this->_client->listBuckets());

        $options = (new TokenCreateOptions())
            ->setDescription('Out buckets manage token')
            ->setCanManageBuckets(true)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // token getter
        $this->assertEquals($client->getTokenString(), $token['token']);
        $this->assertEmpty($token['expires']);
        $this->assertFalse($token['isExpired']);

        $this->assertCount($bucketsInitialCount, $token['bucketPermissions']);
        foreach ($token['bucketPermissions'] as $bucketId => $permission) {
            $this->assertEquals(TokenUpdateOptions::ALL_BUCKETS_PERMISSION, $permission);
        }

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount($bucketsInitialCount, $buckets);

        // create new bucket with master token
        $newBucketId = $this->_client->createBucket('test', 'in', 'testing');

        // check if new token has access to token
        $buckets = $client->listBuckets();
        $this->assertCount($bucketsInitialCount + 1, $buckets);

        $token = $this->_client->getToken($tokenId);
        $this->assertCount($bucketsInitialCount + 1, $token['bucketPermissions']);
        foreach ($token['bucketPermissions'] as $bucketId => $permission) {
            $this->assertEquals(TokenUpdateOptions::ALL_BUCKETS_PERMISSION, $permission);
        }

        $client->getBucket($newBucketId);
        $client->dropBucket($newBucketId);
    }

    public function testKeenReadTokensRetrieve()
    {
        $keen = $this->_client->getKeenReadCredentials();

        $this->assertArrayHasKey('keenToken', $keen);
        $this->assertNotEmpty($keen['keenToken']);
        $this->assertNotEmpty($keen['projectId']);
    }

    public function testInvalidToken()
    {
        $invalidToken = 'thisIsInvalidToken';

        try {
            $client = new \Keboola\StorageApi\Client(array(
                'token' => $invalidToken,
                'url' => STORAGE_API_URL,
            ));

            $client->verifyToken();
            $this->fail('Exception should be thrown on invalid token');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertNotContains($invalidToken, $e->getMessage(), "Token value should not be returned back");
        }
    }

    public function testTokenProperties()
    {
        $currentToken = $this->_client->verifyToken();

        $this->assertArrayHasKey('created', $currentToken);
        $this->assertArrayHasKey('refreshed', $currentToken);
        $this->assertArrayHasKey('description', $currentToken);
        $this->assertArrayHasKey('id', $currentToken);

        $this->assertTrue($currentToken['isMasterToken']);
        $this->assertTrue($currentToken['canManageBuckets']);
        $this->assertTrue($currentToken['canReadAllFileUploads']);
        $this->assertFalse($currentToken['isDisabled']);
        $this->assertNotEmpty($currentToken['bucketPermissions']);
        $this->assertArrayHasKey('owner', $currentToken);
        $this->assertArrayHasKey('admin', $currentToken);

        $owner = $currentToken['owner'];
        $this->assertInternalType('integer', $owner['dataSizeBytes']);
        $this->assertInternalType('integer', $owner['rowsCount']);
        $this->assertInternalType('boolean', $owner['hasRedshift']);

        $this->assertArrayHasKey('limits', $owner);
        $this->assertArrayHasKey('metrics', $owner);
        $this->assertArrayHasKey('defaultBackend', $owner);

        $firstLimit = reset($owner['limits']);
        $limitKeys = array_keys($owner['limits']);

        $this->assertArrayHasKey('name', $firstLimit);
        $this->assertArrayHasKey('value', $firstLimit);
        $this->assertInternalType('int', $firstLimit['value']);
        $this->assertEquals($firstLimit['name'], $limitKeys[0]);

        $tokenFound = false;
        foreach ($this->_client->listTokens() as $token) {
            if ($token['id'] !== $currentToken['id']) {
                continue;
            }

            $this->assertArrayHasKey('admin', $token);

            $admin = $token['admin'];
            $this->assertArrayHasKey('id', $admin);
            $this->assertArrayHasKey('name', $admin);

            $tokenFound = true;
        }

        $this->assertTrue($tokenFound);
    }

    public function testBucketReadTokenPermission()
    {
        $outTableId = $this->_client->createTable(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('Out read token')
            ->addBucketPermission($this->outBucketId, TokenUpdateOptions::BUCKET_PERMISSION_READ)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount(1, $buckets);

        $bucket = reset($buckets);
        $this->assertEquals($this->outBucketId, $bucket['id']);

        // check assigned tables
        $tables = $client->listTables();
        $this->assertCount(1, $tables);

        $table = reset($tables);
        $this->assertEquals($outTableId, $table['id']);

        // read from table
        $tableData = $client->getTableDataPreview($outTableId);
        $this->assertNotEmpty($tableData);

        // write into table
        try {
            $client->writeTable($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
            $this->fail('Table imported with read token');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // table attribute
        try {
            $client->setTableAttribute($outTableId, 'my', 'value');
            $this->fail('Table attribute written with read token');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    public function testBucketWriteTokenPermission()
    {
        $outTableId = $this->_client->createTable(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('Out write token')
            ->addBucketPermission($this->outBucketId, TokenUpdateOptions::BUCKET_PERMISSION_WRITE)
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount(1, $buckets);

        $bucket = reset($buckets);
        $this->assertEquals($this->outBucketId, $bucket['id']);

        // check assigned tables
        $tables = $client->listTables();
        $this->assertCount(1, $tables);

        $table = reset($tables);
        $this->assertEquals($outTableId, $table['id']);

        // read from table
        $tableData = $client->getTableDataPreview($outTableId);
        $this->assertNotEmpty($tableData);

        // write into table
        $client->writeTable($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));

        // table attribute
        $client->setTableAttribute($outTableId, 'my', 'value');
    }

    public function testNoBucketTokenPermission()
    {
        $outTableId = $this->_client->createTable(
            $this->outBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../_data/languages.csv')
        );

        $options = (new TokenCreateOptions())
            ->setDescription('No bucket permission token')
        ;

        $tokenId = $this->_client->createToken($options);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        // check assigned buckets
        $buckets = $client->listBuckets();
        $this->assertCount(0, $buckets);

        // check assigned tables
        $tables = $client->listTables();
        $this->assertCount(0, $tables);

        // read from table
        try {
            $client->getTableDataPreview($outTableId);
            $this->fail('Table exported with no permission token');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // write into table
        try {
            $client->writeTable($outTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
            $this->fail('Table imported with no permission token');
        } catch (ClientException  $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // table attribute
        try {
            $client->setTableAttribute($outTableId, 'my', 'value');
            $this->fail('Table attribute with no permission token');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    public function testAssignNonExistingBucketShouldFail()
    {
        $options = (new TokenCreateOptions())
            ->setDescription('Some description')
            ->addBucketPermission('out.non-existing', TokenUpdateOptions::BUCKET_PERMISSION_READ)
        ;

        try {
            $this->_client->createToken($options);
            $this->fail('Invalid permissions exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tokens.invalidPermissions', $e->getStringCode());
        }
    }
}
