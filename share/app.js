app = angular.module('AgroApp', ['ngRoute'])

app.controller('TaskListController', function($scope, $http) {
		"use strict";

		$scope.url = "/api/task";
		$scope.tasks = [];

		$scope.fetchContent = function() {
			$http.get($scope.url).then(function(result){
				$scope.tasks = result.data;
			});
		}

		$scope.fetchContent();
});

app.controller('WorkerListController', function($scope, $http) {
		"use strict";

		$scope.url = "/api/worker";
		$scope.workers = [];

		$scope.fetchContent = function() {
			$http.get($scope.url).then(function(result){
				$scope.workers = result.data;
			});
		}

		$scope.fetchContent();
});

app.controller('TaskInfoController',
    function($scope, $http, $routeParams) {
        $scope.url = "/api/task/" + $routeParams.task_id

        $scope.task_info = {};

        $scope.fetchContent = function() {
            $http.get($scope.url).then(function(result){
                $scope.task_info = result.data
            })
        }
        $scope.fetchContent();
    }
);


app.controller('JobInfoController',
    function($scope, $http, $routeParams) {
        $scope.url = "/api/job/" + $routeParams.job_id

        $scope.job_info = {};
        $scope.fetchContent = function() {
            $http.get($scope.url).then(function(result){
                $scope.job_info = result.data
            })
        }
        $scope.fetchContent();
    }
);

app.config(['$routeProvider',
    function($routeProvider) {
        $routeProvider.when('/', {
           templateUrl: 'static/list.html',
        }).
        when('/tasks/:task_id', {
           templateUrl: 'static/tasks.html'
        }).
        when('/jobs/:job_id', {
           templateUrl: 'static/jobs.html'
        })
    }
]);