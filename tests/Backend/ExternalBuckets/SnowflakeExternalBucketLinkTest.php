<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Exception;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\ConnectionUtils;
use const STORAGE_API_LINKING_TOKEN;
use const STORAGE_API_SHARE_TOKEN;

class SnowflakeExternalBucketLinkTest extends BaseExternalBuckets
{
    use WorkspaceConnectionTrait;
    use ConnectionUtils;

    public const EXTERNAL_DB = 'EXT_DB_LINK';
    public const EXTERNAL_SCHEMA = 'EXT_SCHEMA_LINK';
    public const EXTERNAL_TABLE = 'EXT_TABLE_LINK';

    /** @var Client */
    protected $shareClient;

    /**
     * @var Client
     */
    protected $linkingClient;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
        $this->shareClient = $this->getClientForToken(
            STORAGE_API_SHARE_TOKEN,
        );
        $this->linkingClient = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );
    }

    public function testShareExternalBucket(): void
    {
        $stage = 'in';
        $bucketName = 'test-bucket-ext-link';

        $this->forceUnshareBucketIfExists($this->shareClient, $stage . '.' . $bucketName, true);
        $this->dropBucketIfExists($this->_client, $stage.'.'.$bucketName, true);

        $guide = $this->_client->registerBucketGuide([self::EXTERNAL_DB, self::EXTERNAL_SCHEMA], 'snowflake');

        $guideExploded = explode("\n", $guide['markdown']);
        $db = $this->ensureSnowflakeConnection();

        $db->executeQuery(
            sprintf(
                'DROP DATABASE IF EXISTS %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_DB),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_SCHEMA),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_SCHEMA),
            ),
        );
        $db->executeQuery(
            sprintf(
                'CREATE TABLE %s (ID INT, LASTNAME VARCHAR(255));',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_TABLE),
            ),
        );
        $db->executeQuery(
            sprintf(
                'USE WAREHOUSE %s',
                SnowflakeQuote::quoteSingleIdentifier('DEV'),
            ),
        );
        $db->executeQuery(
            sprintf(
                'INSERT INTO %s (ID, LASTNAME) VALUES (1, %s)',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_TABLE),
                SnowflakeQuote::quote('Novák'),
            ),
        );

        foreach ($guideExploded as $command) {
            if (str_starts_with($command, 'GRANT') && !str_contains($command, 'FUTURE')) {
                try {
                    $db->executeQuery($command);
                } catch (Exception $e) {
                    $this->fail($e->getMessage() . ': ' . $command);
                }
            }
        }

        $registeredBucketId = $this->_client->registerBucket(
            $bucketName,
            [self::EXTERNAL_DB, self::EXTERNAL_SCHEMA],
            $stage,
            'will not fail',
            'snowflake',
            $bucketName,
        );

        $tables = $this->_client->listTables($stage.'.'.$bucketName);
        $this->assertCount(1, $tables);

        $shareToken = $this->linkingClient->verifyToken();
        $targetProjectId = $shareToken['owner']['id'];

        $this->shareClient->shareBucketToProjects($registeredBucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($registeredBucketId);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertEquals($targetProjectId, $sharedBucket['sharingParameters']['projects'][0]['id']);

        // LINKING START

        $token = $this->_client->verifyToken();
        $linkedBucketId = $this->linkingClient->linkBucket('LINKED_BUCKET', 'in', $token['owner']['id'], $sharedBucket['id'], 'LINKED_BUCKET');
        $linkedBucket = $this->linkingClient->getBucket($linkedBucketId);
        $this->assertEquals($sharedBucket['id'], $linkedBucket['sourceBucket']['id']);

        $linkingTables = $this->linkingClient->listTables($linkedBucketId);
        $this->assertCount(1, $tables);
        $linkingTable = $linkingTables[0];

        $dataPreview = $this->linkingClient->getTableDataPreview($linkingTable['id']);
        $this->assertEquals(
            <<<EXPECTED
"ID","LASTNAME"
"1","Novák"

EXPECTED,
            $dataPreview,
        );

        $this->linkingClient->dropBucket($linkedBucketId, ['force' => true]);
        // LINKING END

        $this->shareClient->unshareBucket($registeredBucketId);
        $unsharedBucket = $this->_client->getBucket($registeredBucketId);
        $this->assertNull($unsharedBucket['sharing']);

        $db->executeQuery(
            sprintf(
                'DROP DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier(self::EXTERNAL_DB),
            ),
        );
    }

    public function testShareWorkspaceBucket(): void
    {
        $stage = 'in';
        $bucketName = 'test-bucket-ext-link';

        $this->forceUnshareBucketIfExists($this->shareClient, $stage . '.' . $bucketName, true);
        $this->dropBucketIfExists($this->_client, $stage.'.'.$bucketName, true);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace([], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable('WORKSPACE_TABLE', ['ID' => 'INT', 'LASTNAME' => 'VARCHAR(255)']);
        $backend->executeQuery(
            sprintf(
                'INSERT INTO %s (ID, LASTNAME) VALUES (1, %s)',
                SnowflakeQuote::quoteSingleIdentifier('WORKSPACE_TABLE'),
                SnowflakeQuote::quote('Novák'),
            ),
        );

        $registeredBucketId = $this->_client->registerBucket(
            $bucketName,
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            $stage,
            'I\'am in workspace',
            'snowflake',
            $bucketName,
        );

        $tables = $this->_client->listTables($stage.'.'.$bucketName);
        $this->assertCount(1, $tables);

        $shareToken = $this->linkingClient->verifyToken();
        $targetProjectId = $shareToken['owner']['id'];

        $this->shareClient->shareBucketToProjects($registeredBucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($registeredBucketId);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertEquals($targetProjectId, $sharedBucket['sharingParameters']['projects'][0]['id']);

        // LINKING START

        $token = $this->_client->verifyToken();
        $linkedBucketId = $this->linkingClient->linkBucket('LINKED_BUCKET', 'in', $token['owner']['id'], $sharedBucket['id'], 'LINKED_BUCKET');
        $linkedBucket = $this->linkingClient->getBucket($linkedBucketId);
        $this->assertEquals($sharedBucket['id'], $linkedBucket['sourceBucket']['id']);

        $linkingTables = $this->linkingClient->listTables($linkedBucketId);
        $this->assertCount(1, $tables);
        $linkingTable = $linkingTables[0];

        $dataPreview = $this->linkingClient->getTableDataPreview($linkingTable['id']);
        $this->assertEquals(
            <<<EXPECTED
"ID","LASTNAME"
"1","Novák"

EXPECTED,
            $dataPreview,
        );

        $this->linkingClient->dropBucket($linkedBucketId, ['force' => true]);
        // LINKING END

        $this->shareClient->unshareBucket($registeredBucketId);
        $unsharedBucket = $this->_client->getBucket($registeredBucketId);
        $this->assertNull($unsharedBucket['sharing']);

        $workspaces->deleteWorkspace($workspace['id']);
    }
}
