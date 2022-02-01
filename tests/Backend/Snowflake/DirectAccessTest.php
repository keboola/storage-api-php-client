<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DirectAccess;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\BucketUpdateOptions;
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
            $this->fail('Should have thrown!');
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
        $this->assertArrayHasKey('warehouse', $newCredentials);
        $this->assertNotNull($newCredentials['warehouse']);

        $credentials = $directAccess->getCredentials($backend);
        $this->assertArrayHasKey('host', $credentials);
        $this->assertArrayHasKey('username', $credentials);
        $this->assertArrayHasKey('warehouse', $credentials);
        $this->assertSame($newCredentials['username'], $credentials['username']);
        $this->assertSame($newCredentials['warehouse'], $credentials['warehouse']);

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
        $newToken = $this->tokens->createToken(new TokenCreateOptions());
        $client = $this->getClient([
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
        $bucketName = 'API-DA-tests';
        $bucket2Name = 'API-DA_TEST';
        $bucketStage = 'in';
        $bucketId = $bucketStage . '.c-' . $bucketName;
        $bucket2Id = $bucketStage . '.c-' . $bucket2Name;

        $tableName = 'mytable';
        $tableId = $bucketId . '.' . $tableName . '';
        $table2Name = 'other_table';
        $table2Id = $bucket2Id . '.' . $table2Name . '';

        $client2 = $this->getClient([
            'token' => STORAGE_API_LINKING_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);

        $linkedBucketName = 'API-linked-tests';
        $linkedBucketStage = 'in';
        $linkedBucketId = $linkedBucketStage . '.c-' . $linkedBucketName;

        $directAccess = new DirectAccess($this->_client);

        $this->prepareDirectAccess($linkedBucketId);
        $this->prepareDirectAccess($bucketId);
        $this->prepareDirectAccess($bucket2Id);
        $this->dropBucketIfExists($client2, $linkedBucketId, true);
        $this->dropBucketIfExists($this->_client, $bucketId);
        $this->dropBucketIfExists($this->_client, $bucket2Id);

        //test drop table with DA enabled via async call
        $bucketId = $this->_client->createBucket($bucketName, $bucketStage, '', null, 'b1-display-name');
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable($bucketId, $tableName, new CsvFile($importFile));

        // test alias
        $aliasTableId = $this->_client->createAliasTable($bucketId, $tableId, 'this-is-alias');
        try {
            $directAccess->enableForBucket($bucketId);
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame('Cannot enable Direct Access for bucket that has alias table ("this-is-alias")', $e->getMessage());
        }
        $this->_client->dropTable($aliasTableId);

        $this->_client->getTable($tableId);
        $directAccess->enableForBucket($bucketId);

        $credentials = $directAccess->createCredentials(self::BACKEND_SNOWFLAKE);

        $connection = new Connection([
            'host' => $credentials['host'],
            'user' => $credentials['username'],
            'password' => $credentials['password'],
        ]);

        $schemas = $connection->fetchAll('SHOW SCHEMAS');
        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'DA_IN_B1-DISPLAY-NAME';
        }));
        $this->assertSame('DA_IN_B1-DISPLAY-NAME', $schemas[0]['name']);

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier($schemas[0]['name'])
        ));

        $viewsResult = $connection->fetchAll('SHOW VIEWS');
        $this->assertCount(1, $viewsResult);
        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'mytable';
        }));
        $this->assertSame('mytable', $views[0]['name']);

        //test drop table with DA enabled
        $this->_client->dropTable($tableId);

        $viewsResult = $connection->fetchAll('SHOW VIEWS');
        $this->assertCount(0, $viewsResult);
        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'mytable';
        }));
        $this->assertEmpty($views);

        try {
            $this->_client->getTable($tableId);
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertContains(
                'The table "mytable" was not found in the bucket "in.c-API-DA-tests"',
                $e->getMessage()
            );
        }

        $this->dropBucketIfExists($this->_client, $bucketId, true);

        //test drop bucket with DA enabled via async call
        $bucketId = $this->_client->createBucket($bucketName, $bucketStage, '', null, 'b1-display-name');
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable($bucketId, $tableName, new CsvFile($importFile));

        $this->_client->getTable($tableId);
        $directAccess->enableForBucket($bucketId);

        $schemas = $connection->fetchAll('SHOW SCHEMAS');
        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'DA_IN_B1-DISPLAY-NAME';
        }));
        $this->assertSame('DA_IN_B1-DISPLAY-NAME', $schemas[0]['name']);

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier($schemas[0]['name'])
        ));

        $viewsResult = $connection->fetchAll('SHOW VIEWS');
        $this->assertCount(1, $viewsResult);
        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'mytable';
        }));
        $this->assertSame('mytable', $views[0]['name']);

        try {
            $bucketUpdateOptions = new BucketUpdateOptions($bucketId, 'updated-b1-display-name', false);
            $this->_client->updateBucket($bucketUpdateOptions);
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Bucket "in.c-API-DA-tests" has Direct Access enabled please use async call',
                $e->getMessage()
            );
        }

        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, 'updated-b1-display-name', true);
        $this->_client->updateBucket($bucketUpdateOptions);

        $schemas = $connection->fetchAll('SHOW SCHEMAS');
        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'DA_IN_UPDATED-B1-DISPLAY-NAME';
        }));
        $this->assertSame('DA_IN_UPDATED-B1-DISPLAY-NAME', $schemas[0]['name']);

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier($schemas[0]['name'])
        ));

        $viewsResult = $connection->fetchAll('SHOW VIEWS');
        $this->assertCount(1, $viewsResult);
        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'mytable';
        }));
        $this->assertSame('mytable', $views[0]['name']);

        $viewsResult = $connection->fetchAll('SELECT * FROM "mytable"');
        $this->assertCount(5, $viewsResult);

        $this->_client->dropBucket($bucketId, ['force' => true, 'async' => true]);

        $viewsResult = $connection->fetchAll('SHOW VIEWS');
        // cannot test count as we don't have the schema anymore and there are some views in DB
        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'mytable';
        }));
        $this->assertEmpty($views);

        $schemas = $connection->fetchAll('SHOW SCHEMAS');
        $this->assertCount(1, $schemas, 'There should be only INFORMATION SCHEMA');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'DA_IN_B1-DISPLAY-NAME';
        }));
        $this->assertEmpty($schemas);

        // cleanup
        $this->dropBucketIfExists($this->_client, $bucketId, true);
        $directAccess->deleteCredentials(self::BACKEND_SNOWFLAKE);

        // test only enabled buckets are visible in DA
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
                'Bucket "in.c-API-DA-tests" has Direct Access enabled please use async call',
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
        $this->assertSame('DA_IN_B1-DISPLAY-NAME', $bucket['directAccessSchemaName']);

        // other buckets does not have DA enabled
        $bucket = $this->_client->getBucket($bucket2Id);
        $this->assertFalse($bucket['directAccessEnabled']);
        $this->assertNull($bucket['directAccessSchemaName']);


        $credentials = $directAccess->createCredentials(self::BACKEND_SNOWFLAKE);

        $connection = new Connection([
            'host' => $credentials['host'],
            'user' => $credentials['username'],
            'password' => $credentials['password'],
        ]);

        $schemas = $connection->fetchAll('SHOW SCHEMAS');
        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'DA_IN_B1-DISPLAY-NAME';
        }));
        $this->assertSame('DA_IN_B1-DISPLAY-NAME', $schemas[0]['name']);

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
            'CREATE OR REPLACE VIEW "DA_IN_B1-DISPLAY-NAME"."mytable_displayName"'
            . ' AS SELECT * FROM "in.c-API-DA-tests"."mytable"',
            $views[0]['text']
        );

        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'newSecondTable';
        }));
        $this->assertSame('newSecondTable', $views[0]['name']);
        $this->assertSame(
            'CREATE OR REPLACE VIEW "DA_IN_B1-DISPLAY-NAME"."newSecondTable"'
            . ' AS SELECT * FROM "in.c-API-DA-tests"."newSecondTable"',
            $views[0]['text']
        );

        $tables = $connection->fetchAll('SHOW TABLES');
        $this->assertSame([], $tables);

        try {
            $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
            $this->_client->writeTable(
                $tableId,
                new CsvFile($importFile),
                [
                    'incremental' => true,
                ]
            );
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Bucket "in.c-API-DA-tests" has Direct Access enabled please use async call',
                $e->getMessage()
            );
        }

        try {
            $this->_client->createAliasTable($bucketId, $tableId, 'tableAlias');
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Cannot add alias to bucket "in.c-API-DA-tests" with direct access enabled, disable direct access first',
                $e->getMessage()
            );
        }

        try {
            $this->_client->updateTable($tableId, ['displayName' => 'differentDisplayName']);
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                'Cannot update a table "in.c-API-DA-tests.mytable" in bucket "in.c-API-DA-tests" with Direct Access enabled please use async call',
                $e->getMessage()
            );
        }

        $connection->query(sprintf(
            'USE DATABASE %s',
            $connection->quoteIdentifier($schemas[0]['database_name'])
        ));

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier($schemas[0]['name'])
        ));
        $views = $connection->fetchAll('SHOW VIEWS');

        $this->assertCount(2, $views);

        $views = array_values(array_filter($views, function ($view) {
            return $view['name'] === 'mytable_displayName';
        }));
        $this->assertSame('mytable_displayName', $views[0]['name']);
        $this->assertSame(
            'CREATE OR REPLACE VIEW "DA_IN_B1-DISPLAY-NAME"."mytable_displayName"'
            . ' AS SELECT * FROM "in.c-API-DA-tests"."mytable"',
            $views[0]['text']
        );

        $this->_client->updateTable($tableId, ['displayName' => 'updatedDisplayName', 'async' => true]);

        $connection->query(sprintf(
            'USE DATABASE %s',
            $connection->quoteIdentifier($schemas[0]['database_name'])
        ));

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier($schemas[0]['name'])
        ));
        $views = $connection->fetchAll('SHOW VIEWS');

        $this->assertCount(2, $views);
        $views = array_values(array_filter($views, function ($view) {
            return $view['name'] === 'updatedDisplayName';
        }));
        $this->assertSame('updatedDisplayName', $views[0]['name']);
        $this->assertSame(
            'CREATE OR REPLACE VIEW "DA_IN_B1-DISPLAY-NAME"."updatedDisplayName"'
            . ' AS SELECT * FROM "in.c-API-DA-tests"."mytable"',
            $views[0]['text']
        );

        $directAccess->disableForBucket($bucketId);
        $directAccess->enableForBucket($bucket2Id);

        $schemas = $connection->fetchAll('SHOW SCHEMAS');
        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'DA_IN_API-DA_TEST';
        }));
        $this->assertSame('DA_IN_API-DA_TEST', $schemas[0]['name']);

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
            'CREATE OR REPLACE VIEW "DA_IN_API-DA_TEST"."other_table"'
            . ' AS SELECT * FROM "in.c-API-DA_TEST"."other_table"',
            $views[0]['text']
        );


        $aliasTableId = $this->_client->createAliasTable($bucketId, $tableId, 'this-is-alias');

        $project2Id = $client2->verifyToken()['owner']['id'];
        //Linked bucket
        $this->_client->shareBucketToProjects($bucketId, [$project2Id]);

        $client2DirectAccess = new DirectAccess($client2);
        try {
            $client2DirectAccess->deleteCredentials(self::BACKEND_SNOWFLAKE);
        } catch (\Keboola\StorageApi\ClientException $e) {
        }

        $client2Credentials = $client2DirectAccess->createCredentials(self::BACKEND_SNOWFLAKE);

        $response = $client2->listSharedBuckets();
        $sourceProjectId = $this->_client->verifyToken()['owner']['id'];
        $sharedBucket = array_filter($response, static function ($v) use ($sourceProjectId) {
            return $v['displayName'] === 'b1-display-name' && $sourceProjectId === $v['project']['id'];
        });
        $sharedBucket = reset($sharedBucket);
        $linkedBucketId = $client2->linkBucket(
            $linkedBucketName,
            $linkedBucketStage,
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        try {
            $client2DirectAccess->enableForBucket($linkedBucketId);
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame('Cannot enable Direct Access for bucket that has alias table ("this-is-alias")'
                .' in source bucket', $e->getMessage());
        }

        $this->_client->dropTable($aliasTableId, ['force'=>true]);

        $client2DirectAccess->enableForBucket($linkedBucketId);

        $client2Connection = new Connection([
            'host' => $client2Credentials['host'],
            'user' => $client2Credentials['username'],
            'password' => $client2Credentials['password'],
        ]);

        $directAccess->enableForBucket($bucketId);

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier('DA_IN_B1-DISPLAY-NAME')
        ));

        $columns = array_map(function ($row) {
            return $row['column_name'];
        }, $connection->fetchAll(sprintf('SHOW COLUMNS IN %s', $connection->quoteIdentifier('updatedDisplayName'))));

        $this->assertEquals(['id', 'name', '_timestamp'], $columns);

        $importFile = __DIR__ . '/../../_data/languages.more-columns.csv';
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($importFile),
            [
                'incremental' => true,
            ]
        );

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier('DA_IN_B1-DISPLAY-NAME')
        ));

        $columns = array_map(function ($row) {
            return $row['column_name'];
        }, $connection->fetchAll(sprintf('SHOW COLUMNS IN %s', $connection->quoteIdentifier('updatedDisplayName'))));

        $this->assertContains('count', $columns);
        $this->assertEquals(['id', 'name', '_timestamp', 'count'], $columns);

        $this->_client->addTableColumn($tableId, 'add_test_column');

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier('DA_IN_B1-DISPLAY-NAME')
        ));

        $columns = array_map(function ($row) {
            return $row['column_name'];
        }, $connection->fetchAll(sprintf('SHOW COLUMNS IN %s', $connection->quoteIdentifier('updatedDisplayName'))));

        $this->assertContains('add_test_column', $columns);
        $this->assertEquals(['id', 'name', '_timestamp', 'count', 'add_test_column'], $columns);

        $schemas = $client2Connection->fetchAll('SHOW SCHEMAS');

        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'DA_IN_API-LINKED-TESTS';
        }));
        $this->assertSame('DA_IN_API-LINKED-TESTS', $schemas[0]['name']);

        $client2Connection->query(sprintf(
            'USE SCHEMA %s',
            $client2Connection->quoteIdentifier($schemas[0]['name'])
        ));

        $columns = array_map(function ($row) {
            return $row['column_name'];
        }, $client2Connection->fetchAll(
            sprintf('SHOW COLUMNS IN %s', $connection->quoteIdentifier('updatedDisplayName'))
        ));

        $this->assertContains('add_test_column', $columns);
        $this->assertEquals(['id', 'name', '_timestamp', 'count', 'add_test_column'], $columns);

        $viewsResult = $client2Connection->fetchAll('SHOW VIEWS');
        $this->assertCount(2, $viewsResult);
        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'newSecondTable';
        }));
        $this->assertSame('newSecondTable', $views[0]['name']);
        $this->assertSame(
            sprintf(
                'CREATE OR REPLACE VIEW "%s"."DA_IN_API-LINKED-TESTS"."newSecondTable"'.
                ' AS SELECT * FROM "%s"."in.c-API-DA-tests"."newSecondTable"',
                $views[0]['database_name'],
                $views[0]['owner']
            ),
            $views[0]['text']
        );

        $views = array_values(array_filter($viewsResult, function ($view) {
            return $view['name'] === 'updatedDisplayName';
        }));
        $this->assertSame('updatedDisplayName', $views[0]['name']);
        $this->assertSame(
            sprintf(
                'CREATE OR REPLACE VIEW "%s"."DA_IN_API-LINKED-TESTS"."updatedDisplayName"'.
                ' AS SELECT * FROM "%s"."in.c-API-DA-tests"."mytable"',
                $views[0]['database_name'],
                $views[0]['owner']
            ),
            $views[0]['text']
        );

        $this->_client->deleteTableColumn($tableId, 'add_test_column', ['force' => true]);

        $connection->query(sprintf(
            'USE SCHEMA %s',
            $connection->quoteIdentifier('DA_IN_B1-DISPLAY-NAME')
        ));

        $columns = array_map(function ($row) {
            return $row['column_name'];
        }, $connection->fetchAll(sprintf('SHOW COLUMNS IN %s', $connection->quoteIdentifier('updatedDisplayName'))));

        $this->assertNotContains('add_test_column', $columns);
        $this->assertEquals(['id', 'name', '_timestamp', 'count'], $columns);

        $schemas = $client2Connection->fetchAll('SHOW SCHEMAS');

        $this->assertCount(2, $schemas, 'There should be INFORMATION SCHEMA and one bucket');
        $schemas = array_values(array_filter($schemas, function ($schema) {
            return $schema['name'] === 'DA_IN_API-LINKED-TESTS';
        }));
        $this->assertSame('DA_IN_API-LINKED-TESTS', $schemas[0]['name']);

        $client2Connection->query(sprintf(
            'USE SCHEMA %s',
            $client2Connection->quoteIdentifier($schemas[0]['name'])
        ));

        $columns = array_map(function ($row) {
            return $row['column_name'];
        }, $client2Connection->fetchAll(
            sprintf('SHOW COLUMNS IN %s', $connection->quoteIdentifier('updatedDisplayName'))
        ));

        $this->assertNotContains('add_test_column', $columns);
        $this->assertEquals(['id', 'name', '_timestamp', 'count'], $columns);

        try {
            $this->_client->forceUnlinkBucket($sharedBucket['id'], $project2Id);
            $this->fail('Should have thrown!');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf(
                    'Linked bucket has Direct Access enabled in project "%s" please use async call',
                    $project2Id
                ),
                $e->getMessage()
            );
        }

        $this->_client->forceUnlinkBucket($sharedBucket['id'], $project2Id, ['async' => true]);

        try {
            $client2->getBucket($sharedBucket['id']);
        } catch (ClientException $e) {
            $this->assertSame('Bucket in.c-API-DA-tests not found', $e->getMessage());
        }

        $schemas = $client2Connection->fetchAll('SHOW SCHEMAS');

        $this->assertCount(1, $schemas, 'There should be INFORMATION SCHEMA');
        $this->assertNotSame('DA_IN_API-LINKED-TESTS', $schemas[0]['name']);

        // validate direct access for dev buckets
        $metadataProvider = Metadata::PROVIDER_SYSTEM;
        $metadataKey = Metadata::BUCKET_METADATA_KEY_ID_BRANCH;

        $this->prepareDirectAccess($bucketId);

        $tableName = 'languages';
        $tableId = $this->_client->createTable(
            $bucketId,
            $tableName,
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $metadata = new Metadata($this->_client);

        // check that validation ignores table/columns metadata
        $metadata->postColumnMetadata(
            sprintf('%s.%s', $tableId, 'id'),
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => '1234',
                ],
            ]
        );

        $metadata->postTableMetadata(
            $tableId,
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => '1234',
                ],
            ]
        );

        $directAccess->enableForBucket($bucketId);

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertTrue($bucket['directAccessEnabled']);

        $directAccess->disableForBucket($bucketId);

        // validate restrictions
        $metadata->postBucketMetadata(
            $bucketId,
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => '1234',
                ],
            ]
        );

        try {
            $directAccess->enableForBucket($bucketId);
            $this->fail('Enabling Direct Access for Dev/Branch bucket should fail');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('Direct Access for Dev/Branch buckets is not supported yet.', $e->getMessage());
        }

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertFalse($bucket['directAccessEnabled']);
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
