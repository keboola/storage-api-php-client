<?php

namespace Keboola\Test\Backend;

use Keboola\Test\StorageApiTestCase;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;

trait WorkspaceCredentialsAssertTrait
{
    private static $RETRY_FAIL_MESSAGE = 'Credentials should be invalid';
    /**
     * @param array $connection
     * @throws \Exception
     */
    private function assertCredentialsShouldNotWork($connection)
    {
        $retryPolicy = new CallableRetryPolicy(function (\Exception $e) {
            return $e->getMessage() === self::$RETRY_FAIL_MESSAGE;
        });
        try {
            $proxy = new RetryProxy($retryPolicy, new ExponentialBackOffPolicy());
            $proxy->call(function () use ($connection) {
                $this->getDbConnection($connection);
                throw new \Exception(self::$RETRY_FAIL_MESSAGE);
            });
        } catch (\Doctrine\DBAL\Driver\Exception $e) {
            switch ($connection['backend']) {
                case StorageApiTestCase::BACKEND_SYNAPSE:
                    $this->assertStringContainsString('Login failed', $e->getMessage());
                    break;
                case StorageApiTestCase::BACKEND_TERADATA:
                    $this->assertContains(
                        $e->getCode(),
                        [
                            1,
                            // Failed call to Logon, aborting connection.
                            130,
                            // TLS connection failed with The legacy port is enabled
                            // but the client failed to connect to it.
                            210,
                            // The UserId, Password or Account is invalid.
                        ],
                        sprintf(
                            'Unexpected error message from Teradata code: "%s" message: "%s".',
                            $e->getCode(),
                            $e->getMessage()
                        )
                    );
                    break;
                default:
                    $this->fail(sprintf(
                        'Unexpected error message from "%s" backend. code: "%s" message: "%s".',
                        $connection['backend'],
                        $e->getCode(),
                        $e->getMessage()
                    ));
            }
        } catch (\PDOException $e) {
            // RS
            $this->assertEquals(7, $e->getCode());
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertStringContainsString('Incorrect username or password was specified', $e->getMessage());
        } catch (\Exception $e) {
            if ($connection['backend'] === StorageApiTestCase::BACKEND_EXASOL) {
                // Exasol authentication failed
                self::assertEquals(
                    -373252,
                    $e->getCode(),
                    'Unexpected error code, expected code for Exasol is -373252.'
                );
            } else {
                throw new \Exception($e->getMessage(), (int) $e->getCode(), $e);
            }
        }
    }
}
