<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Workspaces;

enum PublicKeyName: string
{
    case RSA_PUBLIC_KEY_1 = 'RSA_PUBLIC_KEY';
    case RSA_PUBLIC_KEY_2 = 'RSA_PUBLIC_KEY_2';
}
