<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Options;

use Keboola\StorageApi\Options\GlobalSearchMode;
use Keboola\StorageApi\Options\GlobalSearchOptions;
use PHPUnit\Framework\TestCase;

class GlobalSearchOptionsTest extends TestCase
{
    public function testModeOmittedByDefault(): void
    {
        $params = (new GlobalSearchOptions())->toParamsArray();

        self::assertArrayNotHasKey('mode', $params);
    }

    public function testRegexModeSerialized(): void
    {
        $params = (new GlobalSearchOptions(mode: GlobalSearchMode::REGEX))->toParamsArray();

        self::assertSame('regex', $params['mode']);
    }

    public function testStandardModeSerialized(): void
    {
        $params = (new GlobalSearchOptions(mode: GlobalSearchMode::STANDARD))->toParamsArray();

        self::assertSame('standard', $params['mode']);
    }

    public function testModeCoexistsWithOtherParams(): void
    {
        $params = (new GlobalSearchOptions(
            limit: 10,
            projectIds: [123],
            mode: GlobalSearchMode::REGEX,
        ))->toParamsArray();

        self::assertSame(10, $params['limit']);
        self::assertSame([123], $params['projectIds']);
        self::assertSame('regex', $params['mode']);
    }
}
