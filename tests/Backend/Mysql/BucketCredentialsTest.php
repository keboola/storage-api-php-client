<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */
namespace Keboola\Test\Backend\Mysql;

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
    }

    public function testItShouldNotBeAllowedCreateCredentialsForMysqlBucket()
    {
        try {
            $this->bucketCredentials->createCredentials(
                (new \Keboola\StorageApi\Options\BucketCredentials\CredentialsCreateOptions())
                    ->setBucketId($this->getTestBucketId(self::STAGE_IN))
                    ->setName('testing')
            );
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(501, $e->getCode());
            $this->assertEquals('notImplemented', $e->getStringCode());
        }
    }
}
