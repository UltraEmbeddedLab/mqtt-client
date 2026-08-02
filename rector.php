<?php

declare(strict_types=1);

use Rector\Config\RectorConfig;
use Rector\Php83\Rector\ClassMethod\AddOverrideAttributeToOverriddenMethodsRector;

return RectorConfig::configure()
    ->withPaths([
        __DIR__.'/src',
        __DIR__.'/tests',
        __DIR__.'/tools',
    ])
    ->withRootFiles()
    ->withCache(__DIR__.'/.rector.cache')
    // withPhpSets()/withPreparedSets() replace the deprecated LevelSetList and SetList
    // constants. Same rule coverage, but the target version is declarative — moving to
    // PHP 8.5 later is a one-word change here.
    ->withPhpSets(php84: true)
    ->withPreparedSets(
        deadCode: true,
        codeQuality: true,
        typeDeclarations: true,
        privatization: true,
        earlyReturn: true,
        strictBooleans: true,
    )
    ->withSkip([
        // #[\Override] on interface implementations is noise in a library whose contracts
        // are the point, and it churns every implementor on each interface edit.
        AddOverrideAttributeToOverriddenMethodsRector::class,
    ]);
