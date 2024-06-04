<?php

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Utils\ConnectionUtils;

class SnowflakeBYODBTest extends BaseExternalBuckets
{
    use WorkspaceConnectionTrait;
    use ConnectionUtils;

    public const TESTDB = 'TESTDB';
    public const TESTSCHEMA = 'TESTSCHEMA';

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testRegisterExternalBucketInBYODBEnvironment(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);

        $this->initEvents($this->_client);
        $token = $this->_client->verifyToken();

        // check that this project does not have external buckets feature enabled
        $this->assertFalse(in_array('external-buckets', $token['owner']['features']));
        $guide = $this->_client->registerBucketGuide([self::TESTDB, self::TESTSCHEMA], 'snowflake');

        $guideExploded = explode("\n", $guide['markdown']);
        $db = $this->ensureSnowflakeConnection();

        $db->executeQuery(
            sprintf(
                'DROP DATABASE IF EXISTS %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTDB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTDB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTDB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTSCHEMA),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TESTSCHEMA),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE TABLE %s (ID INT, LASTNAME VARCHAR(255));',
                SnowflakeQuote::quoteSingleIdentifier('TESTTABLE'),
            ),
        );

        foreach ($guideExploded as $command) {
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
    }
}
