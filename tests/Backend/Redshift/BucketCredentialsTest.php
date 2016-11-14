<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */
namespace Keboola\Test\Backend\Redshift;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class BucketCredentialsTest extends StorageApiTestCase
{

    /**
     * @var \Keboola\StorageApi\BucketCredentials
     */
    private $bucketCredentials;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();

        $this->bucketCredentials = new \Keboola\StorageApi\BucketCredentials($this->_client);
        $this->clearCredentials();
    }

    private function clearCredentials()
    {
        foreach ($this->bucketCredentials->listCredentials() as $credentials) {
            $this->bucketCredentials->dropCredentials($credentials['id']);
        }
    }

    public function testCredentialsManipulation()
    {
        $name = 'testing';
        $inBucketId = $this->getTestBucketId(self::STAGE_IN);
        $outBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $this->_client->createTable($inBucketId, 'languages', new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        $this->_client->createTable($outBucketId, 'languages', new CsvFile(__DIR__ . '/../../_data/languages.csv'));

        $credentials = $this->bucketCredentials->createCredentials(
            (new \Keboola\StorageApi\Options\BucketCredentials\CredentialsCreateOptions())
                ->setBucketId($inBucketId)
                ->setName($name)
        );

        $this->assertArrayHasKey('id', $credentials);
        $this->assertEquals($name, $credentials['name']);
        $this->assertArrayHasKey('redshift', $credentials);

        $pdo = $this->createDbConnection($credentials['redshift']);

        // we can query the table
        $pdo->query("SELECT * FROM \"{$credentials['redshift']['schemaName']}\".\"languages\"")->fetchAll();

        try {
            $pdo->query("SELECT * FROM \"{$outBucketId}\".\"languages\"")->fetchAll();
            $this->fail("query should fail");
        } catch (\Exception $e) {
            $this->assertEquals(42501, $e->getCode());
        }

        // drop credentials
        $this->bucketCredentials->dropCredentials($credentials['id']);

        try {
            $this->createDbConnection($credentials['redshift']);
            $this->fail('Credentials should be inactive');
        } catch (\Exception $e) {
            $this->assertRegExp('/password authentication failed/', $e->getMessage());
        }
    }

    public function testCredentialsGet()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $newCredentials = $this->bucketCredentials->createCredentials(
            (new \Keboola\StorageApi\Options\BucketCredentials\CredentialsCreateOptions())
                ->setBucketId($bucketId)
                ->setName('something')
        );

        $credentials = $this->bucketCredentials->getCredentials($newCredentials['id']);
        $this->assertEquals($newCredentials['id'], $credentials['id']);
        $this->assertArrayHasKey('redshift', $credentials);

        $redshiftParams = $credentials['redshift'];

        $this->assertArrayHasKey('host', $redshiftParams);
        $this->assertArrayHasKey('userName', $redshiftParams);
        $this->assertArrayHasKey('port', $redshiftParams);
        $this->assertArrayHasKey('databaseName', $redshiftParams);
        $this->assertArrayHasKey('schemaName', $redshiftParams);
        $this->assertArrayNotHasKey('password', $redshiftParams);

        $this->assertEquals($bucketId, $credentials['bucket']['id']);
    }

    public function testListCredentials()
    {
        $inBucketId = $this->getTestBucketId(self::STAGE_IN);
        $outBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $this->assertCount(0, $this->bucketCredentials->listCredentials());
        $this->assertCount(0, $this->bucketCredentials->listCredentials(
            (new \Keboola\StorageApi\Options\BucketCredentials\ListCredentialsOptions())
                ->setBucketId($inBucketId)
        ));
        $this->assertCount(0, $this->bucketCredentials->listCredentials(
            (new \Keboola\StorageApi\Options\BucketCredentials\ListCredentialsOptions())
                ->setBucketId($outBucketId)
        ));

        $inCredentials = $this->bucketCredentials->createCredentials(
            (new \Keboola\StorageApi\Options\BucketCredentials\CredentialsCreateOptions())
                ->setBucketId($inBucketId)
                ->setName('something')
        );

        $this->bucketCredentials->createCredentials(
            (new \Keboola\StorageApi\Options\BucketCredentials\CredentialsCreateOptions())
                ->setBucketId($outBucketId)
                ->setName('out')
        );
        $credentialsList = $this->bucketCredentials->listCredentials();
        $this->assertCount(2, $credentialsList);

        $firstCredentias = $credentialsList[1];

        $this->assertEquals($firstCredentias['id'], $inCredentials['id']);
        $this->assertArrayHasKey('redshift', $firstCredentias);

        $redshiftParams = $firstCredentias['redshift'];

        $this->assertArrayHasKey('host', $redshiftParams);
        $this->assertArrayHasKey('userName', $redshiftParams);
        $this->assertArrayHasKey('port', $redshiftParams);
        $this->assertArrayHasKey('databaseName', $redshiftParams);
        $this->assertArrayHasKey('schemaName', $redshiftParams);
        $this->assertArrayNotHasKey('password', $redshiftParams);


        $credentialsList = $this->bucketCredentials->listCredentials(
            (new \Keboola\StorageApi\Options\BucketCredentials\ListCredentialsOptions())
                ->setBucketId($outBucketId)
        );
        $this->assertCount(1, $credentialsList);
        $this->assertEquals($outBucketId, $credentialsList[0]['bucket']['id']);
    }

    public function testPermissions()
    {
        $tokenId = $this->_client->createToken(array(), 'test');
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        $bucketCredentials = new \Keboola\StorageApi\BucketCredentials($client);
        try {
            $bucketCredentials->listCredentials();
            $this->fail('List credentials should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $bucketCredentials->createCredentials((new \Keboola\StorageApi\Options\BucketCredentials\CredentialsCreateOptions())
                ->setBucketId($this->getTestBucketId(self::STAGE_IN))
                ->setName('credentials 01'));
            $this->fail('Create credentials should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    /**
     * @param $credentials
     * @return \PDO
     */
    private function createDbConnection($credentials)
    {
        $pdo = new \PDO(
            "pgsql:dbname={$credentials['databaseName']};port={$credentials['port']};host=" . $credentials['host'],
            $credentials['userName'],
            $credentials['password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        return $pdo;
    }
}
