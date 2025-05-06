<?php

declare(strict_types=1);

namespace Keboola\Test;

use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class WorkspaceLoginTypeTest extends TestCase
{
    public static function provideIsPasswordLoginTestData(): iterable
    {
        yield 'default' => [
            'loginType' => WorkspaceLoginType::DEFAULT,
            'isPasswordLogin' => true,
        ];

        yield 'snowflake-person-sso' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_SSO,
            'isPasswordLogin' => false,
        ];

        yield 'snowflake-legacy-service' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD,
            'isPasswordLogin' => true,
        ];

        yield 'snowflake-person-keypair' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
            'isPasswordLogin' => false,
        ];

        yield 'snowflake-service-keypair' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            'isPasswordLogin' => false,
        ];
    }

    /** @dataProvider provideIsPasswordLoginTestData */
    public function testIsPasswordLogin(WorkspaceLoginType $loginType, bool $isPasswordLogin): void
    {
        self::assertSame($isPasswordLogin, $loginType->isPasswordLogin());
    }

    public static function provideIsKeyPairLoginTestData(): iterable
    {
        yield 'default' => [
            'loginType' => WorkspaceLoginType::DEFAULT,
            'isKeyPairLogin' => false,
        ];

        yield 'snowflake-person-sso' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_SSO,
            'isKeyPairLogin' => false,
        ];

        yield 'snowflake-legacy-service' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD,
            'isKeyPairLogin' => false,
        ];

        yield 'snowflake-person-keypair' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
            'isKeyPairLogin' => true,
        ];

        yield 'snowflake-service-keypair' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            'isKeyPairLogin' => true,
        ];
    }

    /** @dataProvider provideIsKeyPairLoginTestData */
    public function testIsKeyPairLogin(WorkspaceLoginType $loginType, bool $isKeyPairLogin): void
    {
        self::assertSame($isKeyPairLogin, $loginType->isKeyPairLogin());
    }
}
