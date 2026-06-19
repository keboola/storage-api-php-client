<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options;

enum GlobalSearchMode: string
{
    case STANDARD = 'standard';
    case REGEX = 'regex';
}
