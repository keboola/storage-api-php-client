<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Client;
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
            $this->assertEquals('storage.directAccess.onlyAdminTokenCanCreateCredentials', $e->getStringCode());
        }
    }

    /** @return DirectAccess */
    private function prepareDirectAccess()
    {
        $directAccess = new DirectAccess($this->_client);

        try {
            if ($directAccess->getCredentials(self::BACKEND_SNOWFLAKE)) {
                $directAccess->deleteCredentials(self::BACKEND_SNOWFLAKE);
            }
        } catch (\Keboola\StorageApi\ClientException $e) {
        }

        return $directAccess;
    }
}
