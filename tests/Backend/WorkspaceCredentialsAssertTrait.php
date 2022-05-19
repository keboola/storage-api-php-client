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
            $isCorrectErrorCode = false;
            if ($connection['backend'] === StorageApiTestCase::BACKEND_SYNAPSE) {
                $isCorrectErrorCode = in_array((string) $e->getCode(), [
                    //Synapse: https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-a-odbc-error-codes?view=sql-server-ver15
                    '28000', // Invalid authorization specification
                    '08004', // Server rejected the connection
                ], true);
            }
            if ($connection['backend'] === StorageApiTestCase::BACKEND_TERADATA) {
                /** @phpstan-ignore-next-line */
                $isCorrectErrorCode = in_array($e->getCode(), [
                    1, // Failed call to Logon, aborting connection.
                    130, // TLS connection failed with The legacy port is enabled, but the client failed to connect to it.
                    210, // The UserId, Password or Account is invalid.
                ], true);
            }
            if (!$isCorrectErrorCode) {
                $this->fail(sprintf('Unexpected error code "%s" for %s credentials fail.', $e->getCode(), ucfirst($connection['backend'])));
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
