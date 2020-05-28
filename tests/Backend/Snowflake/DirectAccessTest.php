<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DirectAccess;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;

class DirectAccessTest extends StorageApiTestCase
{
    public function testGetDirectAccessCredentials()
    {
        $backend = self::BACKEND_SNOWFLAKE;
        $directAccess = $this->prepareDirectAccess();

        try {
            $directAccess->getCredentials($backend);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.directAccess.credentialsForProjectBackendNotFound', $e->getStringCode());
        }

        try {
            $directAccess->resetPassword($backend);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.directAccess.tryResetPasswordOnNonExistCredentials', $e->getStringCode());
        }

        try {
            $directAccess->deleteCredentials($backend);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.directAccess.tryRemoveNonExistCredentials', $e->getStringCode());
        }

        try {
            $directAccess->createCredentials('not-allowed-backend');
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('Invalid request', $e->getMessage());
            $this->assertEquals('storage.directAccess.validationError', $e->getStringCode());
        }

        $newCredentials = $directAccess->createCredentials($backend);

        $this->assertArrayHasKey('host', $newCredentials);
        $this->assertArrayHasKey('username', $newCredentials);
        $this->assertArrayHasKey('password', $newCredentials);

        $credentials = $directAccess->getCredentials($backend);
        $this->assertArrayHasKey('username', $credentials);
        $this->assertSame($newCredentials['username'], $credentials['username']);

        $connection = new Connection([
            'host' => $newCredentials['host'],
            'user' => $newCredentials['username'],
            'password' => $newCredentials['password']
        ]);

        $testResult = $connection->fetchAll("select 'test'");
        $this->assertSame('test', reset($testResult[0]));

        unset($connection);

        $response = $directAccess->resetPassword($backend);

        try {
            new Connection([
                'host' => $newCredentials['host'],
                'user' => $newCredentials['username'],
                'password' => $newCredentials['password']
            ]);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertContains(
                'Incorrect username or password was specified., SQL state 28000 in SQLConnect',
                $e->getMessage()
            );
        }

        $connection = new Connection([
            'host' => $newCredentials['host'],
            'user' => $newCredentials['username'],
            'password' => $response['password']
        ]);

        $testResult = $connection->fetchAll("select 'test'");
        $this->assertSame('test', reset($testResult[0]));

        $this->assertArrayHasKey('password', $response);

        $directAccess->deleteCredentials($backend);

        try {
            $directAccess->getCredentials($backend);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.directAccess.credentialsForProjectBackendNotFound', $e->getStringCode());
        }
    }

    public function testWithNonAdminToken()
    {
        $backend = self::BACKEND_SNOWFLAKE;
        $newTokenId = $this->_client->createToken(new TokenCreateOptions());
        $newToken = $this->_client->getToken($newTokenId);
        $client = new Client([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL,
        ]);

        $directAccess = new DirectAccess($client);

        try {
            $directAccess->createCredentials($backend);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.directAccess.tokenCanNotUseDirectAccess', $e->getStringCode());
        }
    }

    public function testDirectAccessAndRestrictions()
    {
        $bucketName = 'API-tests';
        $bucket2Name = 'API-DA_TEST';
        $bucketStage = 'in';
        $bucketId = $bucketStage . '.c-' . $bucketName;
        $bucket2Id = $bucketStage . '.c-' . $bucket2Name;

        $tableName = 'mytable';
        $tableId = $bucketId . '.' . $tableName . '';
        $table2Name = 'other_table';
        $table2Id = $bucket2Id . '.' . $table2Name . '';

        $directAccess = new DirectAccess($this->_client);

        $this->prepareDirectAccess($bucketId);
        $this->prepareDirectAccess($bucket2Id);
        $this->dropBucketIfExists($this->_client, $bucketId);
        $this->dropBucketIfExists($this->_client, $bucket2Id);

        //test drop bucket with DA enabled via async call
        $bucketId = $this->_client->createBucket($bucketName, $bucketStage, '', null, 'b1-display-name');
        $directAccess->enableForBucket($bucketId);
        $this->dropBucketIfExists($this->_client, $bucketId, true);

        $bucketId = $this->_client->createBucket($bucketName, $bucketStage, '', null, 'b1-display-name');
        $bucket2Id = $this->_client->createBucket($bucket2Name, $bucketStage);


        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable($bucketId, $tableName, new CsvFile($importFile));
        $this->_client->updateTable($tableId, ['displayName' => 'mytable_displayName']);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable($bucket2Id, $table2Name, new CsvFile($importFile));

        $directAccess->enableForBucket($bucketId);

        try {
            $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
            $this->_client->createTable(
                $bucketId,
                'newtable',
                new CsvFile($importFile)
            );
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Bucket "in.c-API-tests" has Direct Access enabled please use async call',
                $e->getMessage()
            );
        }

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $this->_client->createTableAsync(
            $bucketId,
            'newSecondTable',
            new CsvFile($importFile)
        );

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertTrue($bucket['directAccessEnabled']);
        $this->assertSame('da_in_b1-display-name', $bucket['directAccessSchemaName']);

        // other buckets does not have DA enabled
        $bucket = $this->_client->getBucket($bucket2Id);
        $this->assertFalse($bucket['directAccessEnabled']);
        $this->assertSame(null, $bucket['directAccessSchemaName']);


        $credentials = $directAccess->createCredentials(self::BACKEND_SNOWFLAKE);

        $connection = new Connection([
            'host' => $credentials['host'],
            'user' => $credentials['username'],
            'password' => $credentials['password'],
        ]);

        $schemas = $connection->fetchAll('SHOW SCHEMAS');
        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'da_in_b1-display-name';
        }));
        $this->assertSame('da_in_b1-display-name', $schemas[0]['name']);

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier($schemas[0]['name'])
        ));
        $viewsResult = $connection->fetchAll('SHOW VIEWS');
        $this->assertCount(2, $viewsResult);
        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'mytable_displayName';
        }));
        $this->assertSame('mytable_displayName', $views[0]['name']);
        $this->assertSame(
            'CREATE OR REPLACE VIEW "da_in_b1-display-name"."mytable_displayName"'
            . ' AS SELECT * FROM "in.c-API-tests"."mytable"',
            $views[0]['text']
        );

        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'newSecondTable';
        }));
        $this->assertSame('newSecondTable', $views[0]['name']);
        $this->assertSame(
            'CREATE OR REPLACE VIEW "da_in_b1-display-name"."newSecondTable"'
            . ' AS SELECT * FROM "in.c-API-tests"."newSecondTable"',
            $views[0]['text']
        );

        $tables = $connection->fetchAll('SHOW TABLES');
        $this->assertSame([], $tables);

        try {
            $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($importFile),
                [
                    'incremental' => true,
                ]
            );
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Cannot add columns ("iso", "Something" to a table "in.c-API-tests.mytable" in bucket "in.c-API-tests"'
                . ' with direct access enabled, disable direct access first',
                $e->getMessage()
            );
        }

        try {
            $this->_client->addTableColumn($tableId, 'otherColumn');
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Cannot add column to a table "in.c-API-tests.mytable" in bucket "in.c-API-tests" with direct access '
                . 'enabled, disable direct access first',
                $e->getMessage()
            );
        }
        try {
            $this->_client->deleteTableColumn($tableId, 'otherColumn');
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Cannot remove column from a table "in.c-API-tests.mytable" in bucket "in.c-API-tests" with direct '
                . 'access enabled, disable direct access first',
                $e->getMessage()
            );
        }
        try {
            $this->_client->createAliasTable($bucketId, $tableId, 'tableAlias');
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Cannot add alias to bucket "in.c-API-tests" with direct access enabled, disable direct access first',
                $e->getMessage()
            );
        }
        try {
            $this->_client->updateTable($tableId, ['displayName' => 'differentDisplayName']);
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Cannot change displayName of table "in.c-API-tests.mytable" in bucket "in.c-API-tests" with direct '
                . 'access enabled, disable direct access first',
                $e->getMessage()
            );
        }

        $directAccess->disableForBucket($bucketId);
        $directAccess->enableForBucket($bucket2Id);

        $schemas = $connection->fetchAll('SHOW SCHEMAS');
        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'da_in_API-DA_TEST';
        }));
        $this->assertSame('da_in_API-DA_TEST', $schemas[0]['name']);

        $connection->query(sprintf(
            'USE DATABASE %s',
            $connection->quoteIdentifier($schemas[0]['database_name'])
        ));

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier($schemas[0]['name'])
        ));
        $views = $connection->fetchAll('SHOW VIEWS');
        $this->assertCount(1, $views);
        $views = array_values(array_filter($views, function ($view) {
            return $view['name'] === 'other_table';
        }));
        $this->assertSame('other_table', $views[0]['name']);
        $this->assertSame(
            'CREATE OR REPLACE VIEW "da_in_API-DA_TEST"."other_table"'
            . ' AS SELECT * FROM "in.c-API-DA_TEST"."other_table"',
            $views[0]['text']
        );
    }

    /** @return DirectAccess */
    private function prepareDirectAccess($bucketId = null)
    {
        $directAccess = new DirectAccess($this->_client);

        if ($bucketId) {
            try {
                $directAccess->disableForBucket($bucketId);
            } catch (ClientException $e) {
                // intentionally empty
            }
        }

        try {
            if ($directAccess->getCredentials(self::BACKEND_SNOWFLAKE)) {
                $directAccess->deleteCredentials(self::BACKEND_SNOWFLAKE);
            }
        } catch (\Keboola\StorageApi\ClientException $e) {
        }

        return $directAccess;
    }
}
