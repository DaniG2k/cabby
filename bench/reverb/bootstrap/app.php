<?php

// The smallest Laravel application that runs `php artisan reverb:start`: no routes, no database,
// no views. Configuration is Reverb's packaged config/reverb.php, read from the environment the `sut`
// wrapper sets.

use Illuminate\Foundation\Application;
use Illuminate\Foundation\Configuration\Exceptions;
use Illuminate\Foundation\Configuration\Middleware;

return Application::configure(basePath: dirname(__DIR__))
    ->withMiddleware(function (Middleware $middleware): void {
        //
    })
    ->withExceptions(function (Exceptions $exceptions): void {
        //
    })
    ->create();
