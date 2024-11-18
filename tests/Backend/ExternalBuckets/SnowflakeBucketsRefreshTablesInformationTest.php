<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Utils\ConnectionUtils;

class SnowflakeBucketsRefreshTablesInformationTest extends BaseExternalBuckets
{
    use ConnectionUtils;

    const BUCKET_FINANCE = 'test-finance';
    const BUCKET_ACCOUNTING = 'test-accounting';
    public function testRefreshTablesInformationEndpointExists(): void
    {
        $bucketId = $this->initEmptyBucketWithDescription(self::STAGE_IN);
        $bucket = $this->_client->getBucket($bucketId);

        $bClient = $this->getBranchAwareDefaultClient($bucket['idBranch']);
        $bClient->refreshTableInformationInBucket($bucketId);
        $this->assertTrue(true, 'Testing only if requests working.');

        $this->dropBucketIfExists($this->_client, $bucketId);
    }

    public function testRefreshTablesInformation(): void
    {
        $createdTableRows = 10;

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => self::BACKEND_SNOWFLAKE,
            ],
        );

        $bucketId = $this->initEmptyBucketWithDescription(self::STAGE_IN);
        $bucket = $this->_client->getBucket($bucketId);

        $tableId = $this->createTableWithRandomData('refresh-tables-information-test-table', $createdTableRows, 2);
        $aliasId = $this->_client->createAliasTable($bucketId, $tableId, 'alias');
        $table = $this->_client->getTable($tableId);

        $rowsCount = $table['rowsCount'];
        $dataSizeBytes = $table['dataSizeBytes'];

        $this->assertEquals($createdTableRows, $rowsCount);

        $db = $this->ensureSnowflakeConnection();

        $db->executeQuery(
            sprintf(
                'GRANT ROLE %s TO USER %s;',
                SnowflakeQuote::quoteSingleIdentifier($workspace['connection']['database']),
                SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_USER')),
            ),
        );

        $db->executeQuery(
            sprintf(
                'USE ROLE %s;',
                SnowflakeQuote::quoteSingleIdentifier($workspace['connection']['database']),
            ),
        );

        $db->executeQuery(
            sprintf(
                'USE WAREHOUSE %s;',
                SnowflakeQuote::quoteSingleIdentifier($workspace['connection']['warehouse']),
            ),
        );

        $db->executeQuery(
            sprintf(
                'USE DATABASE %s;',
                SnowflakeQuote::quoteSingleIdentifier($workspace['connection']['database']),
            ),
        );

        $db->executeQuery(
            sprintf(
                'USE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier($bucketId),
            ),
        );

        $db->executeQuery(
            sprintf(
                'INSERT INTO %s ("col_1","col_2") VALUES (\'testvalue 1\', \'testvalue 2\'),(\'testvalue 1\', \'testvalue 2\')',
                SnowflakeQuote::quoteSingleIdentifier($table['name']),
            ),
        );

        $bClient = $this->getBranchAwareDefaultClient($bucket['idBranch']);

        $bClient->refreshTableInformationInBucket($bucketId);

        $refreshedTable = $this->_client->getTable($tableId);
        $refreshedTableAlias = $this->_client->getTable($aliasId);

        $this->assertEquals($rowsCount + 2, $refreshedTable['rowsCount']);
        $this->assertEquals($refreshedTable['rowsCount'], $refreshedTableAlias['rowsCount']);
        // TODO: check dataSizeBytes did not change but rowsCount did
//        $this->assertNotEquals($dataSizeBytes, $refreshedTable['dataSizeBytes']);
//        $this->assertNotEquals($refreshedTable['dataSizeBytes'], $refreshedTableAlias['dataSizeBytes']);

        $this->_client->dropTable($tableId, ['force' => true]);
        $this->_client->dropBucket($bucketId);
        $workspaces->deleteWorkspace($workspace['id']);

        $db->executeQuery(
            sprintf(
                'USE ROLE %s;',
                SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_USER')),
            ),
        );

        $db->executeQuery(
            sprintf(
                'REVOKE ROLE %s FROM USER %s;',
                SnowflakeQuote::quoteSingleIdentifier($workspace['connection']['database']),
                SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_USER')),
            ),
        );
    }

    public function testAliasesShouldNotBeRefreshedWhenSourceTableIsInAnotherBucket(): void
    {
        $expectedRowsForTableAndAlias = 6;

        $client = $this->getBranchAwareDefaultClient('default');
        foreach ([self::BUCKET_FINANCE, self::BUCKET_ACCOUNTING] as $bucketName) {
            $this->dropBucketIfExists($this->_client, 'in.c-' . $bucketName);
        }
        $bucketIdFinance = $this->_client->createBucket(self::BUCKET_FINANCE, self::STAGE_IN);
        $bucketIdAccountingWithAliasSource = $this->_client->createBucket(self::BUCKET_ACCOUNTING, self::STAGE_IN);

        $tableInAccountingBucket = $this->createTableWithRandomData(
            'invoices',
            $expectedRowsForTableAndAlias,
            3,
            bucketId: $bucketIdAccountingWithAliasSource,
        );

        $this->createTableWithRandomData(
            'invoices',
            2,
            3,
            bucketId: $bucketIdFinance,
        );

        // creating alias from ACCOUNTING but in FINANCE bucket
        $createdAlias = $this->_client->createAliasTable($bucketIdFinance, $tableInAccountingBucket, 'invoices_alias');
        $aliasDetail = $this->_client->getTable($createdAlias);
        $this->assertEquals($expectedRowsForTableAndAlias, $aliasDetail['rowsCount']);
        $client->refreshTableInformationInBucket($bucketIdFinance);
        $aliasDetail = $this->_client->getTable($createdAlias);
        // the alias should keep the old row count
        $this->assertEquals($expectedRowsForTableAndAlias, $aliasDetail['rowsCount']);
    }
}
