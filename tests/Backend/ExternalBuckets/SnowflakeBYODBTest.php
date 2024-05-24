<?php

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;

class SnowflakeBYODBTest extends BaseExternalBuckets
{
    use WorkspaceConnectionTrait;

    public const TESTDB = 'TESTDB';
    public const TESTSCHEMA = 'TESTSCHEMA';
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testRegisterExternalBucketInBYODBEnvironment(): void
    {
        // TODO remove when EB grants are being removed based on metadata
        $ws = new Workspaces($this->_client);
        $wsObj = $ws->createWorkspace();

        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);

        $this->initEvents($this->_client);
        $token = $this->_client->verifyToken();

        // check that this project does not have external buckets feature enabled
        $this->assertFalse(in_array('external-buckets', $token['owner']['features']));
        $guide = $this->_client->registerBucketGuide([self::TESTDB, self::TESTSCHEMA], 'snowflake');

        $guideEploded = explode("\n", $guide['markdown']);

        $db = SnowflakeConnectionFactory::getConnection(
            getenv('SNOWFLAKE_HOST'),
            getenv('SNOWFLAKE_USER'),
            getenv('SNOWFLAKE_PASSWORD'),
            []
        );

        $db->executeQuery(
            sprintf(
                'DROP DATABASE IF EXISTS %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTDB),
            )
        );
        $db->executeQuery(
            sprintf(
                'CREATE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTDB),
            )
        );
        $db->executeQuery(
            sprintf(
                'USE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTDB),
            )
        );
        $db->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTSCHEMA),
            )
        );
        $db->executeQuery(
            sprintf(
                'USE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTSCHEMA),
            )
        );
        $db->executeQuery(
            sprintf(
                'CREATE TABLE %s (ID INT, LASTNAME VARCHAR(255));',
                SnowflakeQuote::quoteSingleIdentifier('TESTTABLE'),
            )
        );

        foreach ($guideEploded as $command) {
            if (str_starts_with($command, 'GRANT') && !str_contains($command, 'FUTURE')) {
                $db->executeQuery($command);
            }
        }

        $this->_client->registerBucket(
            'test-bucket-registration',
            [self::TESTDB, self::TESTSCHEMA],
            'in',
            'will not fail',
            'snowflake',
            'test-bucket-registration',
        );

        $tables = $this->_client->listTables('in.test-bucket-registration');
        $this->assertCount(1, $tables);
        $this->_client->refreshBucket('in.test-bucket-registration');
        // TODO remove when EB grants are being removed based on metadata
        $ws->deleteWorkspace($wsObj['id']);
    }
}
