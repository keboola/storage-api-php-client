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

    public const TEST_DB = 'TEST_DB';
    public const TEST_SCHEMA = 'TEST_SCHEMA';
    public const TEST_TABLE = 'TEST_TABLE';
    public const TEST_VIEW = 'TEST_VIEW';

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testRegisterExternalBucketInBYODBEnvironment(): void
    {
        $bucketId = 'in.test-bucket-registration';
        $this->dropBucketIfExists($this->_client, $bucketId, true);

        $this->initEvents($this->_client);
        $token = $this->_client->verifyToken();

        // check that this project does not have external buckets feature enabled
        $this->assertFalse(in_array('external-buckets', $token['owner']['features']));
        $guide = $this->_client->registerBucketGuide([self::TEST_DB, self::TEST_SCHEMA], 'snowflake');

        $guideExploded = explode("\n", $guide['markdown']);
        $db = $this->ensureSnowflakeConnection();

        $db->executeQuery(
            sprintf(
                'DROP DATABASE IF EXISTS %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE TABLE %s (ID INT, LASTNAME VARCHAR(255));',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_TABLE),
            ),
        );

        foreach ($guideExploded as $command) {
            if (str_starts_with($command, 'GRANT') && !str_contains($command, 'FUTURE')) {
                $db->executeQuery($command);
            }
        }

        $this->_client->registerBucket(
            'test-bucket-registration',
            [self::TEST_DB, self::TEST_SCHEMA],
            'in',
            'will not fail',
            'snowflake',
            'test-bucket-registration',
        );

        $tables = $this->_client->listTables($bucketId);
        $this->assertCount(2, $tables);

        $db->executeQuery(
            sprintf(
                'DROP VIEW %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_VIEW),
            ),
        );

        $this->_client->refreshBucket($bucketId);

        $tables = $this->_client->listTables($bucketId);
        $this->assertCount(1, $tables);

        $db->executeQuery(
            sprintf(
                'DROP DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_DB),
            ),
        );
    }
}
