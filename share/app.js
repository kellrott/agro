app = angular.module('AgroApp', [])

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