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
        } catch (\Exception $e) {
            if ($e instanceof \Doctrine\DBAL\Driver\PDOException) {
                // Synapse|Exasol
                if (!in_array(
                    $e->getCode(),
                    [
                        //https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-a-odbc-error-codes?view=sql-server-ver15
                        '28000', // Invalid authorization specification
                        '08004', // Server rejected the connection
                    ],
                    true
                )) {
                    self::fail(sprintf('Unexpected error code "%s" for Synapse credentials fail.', $e->getCode()));
                }
            } elseif ($e instanceof \PDOException) {
                // RS
                self::assertEquals(7, $e->getCode());
            } elseif ($e instanceof \Keboola\Db\Import\Exception) {
                self::assertContains('Incorrect username or password was specified', $e->getMessage());
            } elseif ($connection['backend'] === StorageApiTestCase::BACKEND_EXASOL) {
                // Exasol authentication failed
                self::assertEquals(
                    -373252,
                    $e->getCode(),
                    'Unexpected error code, expected code for Exasol is -373252.'
                );
            } else {
                throw $e;
            }
        }
    }
}
