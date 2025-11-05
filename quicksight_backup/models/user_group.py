"""
Data models for QuickSight users and groups.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum


class IdentityType(Enum):
    """QuickSight identity types."""
    IAM = "IAM"
    QUICKSIGHT = "QUICKSIGHT"


class UserRole(Enum):
    """QuickSight user roles."""
    ADMIN = "ADMIN"
    AUTHOR = "AUTHOR"
    READER = "READER"
    RESTRICTED_AUTHOR = "RESTRICTED_AUTHOR"
    RESTRICTED_READER = "RESTRICTED_READER"


@dataclass
class User:
    """QuickSight user data model."""
    
    user_name: str
    arn: str
    email: Optional[str] = None
    role: Optional[str] = None
    identity_type: Optional[str] = None
    active: bool = True
    principal_id: Optional[str] = None
    custom_permissions_name: Optional[str] = None
    external_login_federation_provider_type: Optional[str] = None
    external_login_federation_provider_url: Optional[str] = None
    external_login_id: Optional[str] = None
    backup_timestamp: datetime = field(default_factory=datetime.now)
    
    @classmethod
    def from_quicksight_api(cls, api_response: Dict[str, Any]) -> 'User':
        """
        Create User instance from QuickSight API response.
        
        Args:
            api_response: Dictionary from QuickSight ListUsers API response
            
        Returns:
            User: User instance with populated fields
        """
        return cls(
            user_name=api_response.get('UserName', ''),
            arn=api_response.get('Arn', ''),
            email=api_response.get('Email'),
            role=api_response.get('Role'),
            identity_type=api_response.get('IdentityType'),
            active=api_response.get('Active', True),
            principal_id=api_response.get('PrincipalId'),
            custom_permissions_name=api_response.get('CustomPermissionsName'),
            external_login_federation_provider_type=api_response.get('ExternalLoginFederationProviderType'),
            external_login_federation_provider_url=api_response.get('ExternalLoginFederationProviderUrl'),
            external_login_id=api_response.get('ExternalLoginId'),
            backup_timestamp=datetime.now()
        )
    
    def to_dynamodb_item(self) -> Dict[str, Any]:
        """
        Convert User instance to DynamoDB item format.
        
        Returns:
            Dict[str, Any]: DynamoDB item dictionary
        """
        item = {
            'user_name': self.user_name,
            'arn': self.arn,
            'active': self.active,
            'backup_timestamp': self.backup_timestamp.isoformat()
        }
        
        # Add optional fields if they exist
        if self.email:
            item['email'] = self.email
        if self.role:
            item['role'] = self.role
        if self.identity_type:
            item['identity_type'] = self.identity_type
        if self.principal_id:
            item['principal_id'] = self.principal_id
        if self.custom_permissions_name:
            item['custom_permissions_name'] = self.custom_permissions_name
        if self.external_login_federation_provider_type:
            item['external_login_federation_provider_type'] = self.external_login_federation_provider_type
        if self.external_login_federation_provider_url:
            item['external_login_federation_provider_url'] = self.external_login_federation_provider_url
        if self.external_login_id:
            item['external_login_id'] = self.external_login_id
            
        return item
    
    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'User':
        """
        Create User instance from DynamoDB item.
        
        Args:
            item: DynamoDB item dictionary
            
        Returns:
            User: User instance with populated fields
        """
        backup_timestamp = datetime.fromisoformat(item['backup_timestamp']) if 'backup_timestamp' in item else datetime.now()
        
        return cls(
            user_name=item['user_name'],
            arn=item['arn'],
            email=item.get('email'),
            role=item.get('role'),
            identity_type=item.get('identity_type'),
            active=item.get('active', True),
            principal_id=item.get('principal_id'),
            custom_permissions_name=item.get('custom_permissions_name'),
            external_login_federation_provider_type=item.get('external_login_federation_provider_type'),
            external_login_federation_provider_url=item.get('external_login_federation_provider_url'),
            external_login_id=item.get('external_login_id'),
            backup_timestamp=backup_timestamp
        )


@dataclass
class Group:
    """QuickSight group data model."""
    
    group_name: str
    arn: str
    description: Optional[str] = None
    principal_id: Optional[str] = None
    members: List[str] = field(default_factory=list)
    backup_timestamp: datetime = field(default_factory=datetime.now)
    
    @classmethod
    def from_quicksight_api(cls, api_response: Dict[str, Any], members: Optional[List[str]] = None) -> 'Group':
        """
        Create Group instance from QuickSight API response.
        
        Args:
            api_response: Dictionary from QuickSight ListGroups API response
            members: List of member usernames (from separate API call)
            
        Returns:
            Group: Group instance with populated fields
        """
        return cls(
            group_name=api_response.get('GroupName', ''),
            arn=api_response.get('Arn', ''),
            description=api_response.get('Description'),
            principal_id=api_response.get('PrincipalId'),
            members=members or [],
            backup_timestamp=datetime.now()
        )
    
    def to_dynamodb_item(self) -> Dict[str, Any]:
        """
        Convert Group instance to DynamoDB item format.
        
        Returns:
            Dict[str, Any]: DynamoDB item dictionary
        """
        item = {
            'group_name': self.group_name,
            'arn': self.arn,
            'members': self.members,
            'backup_timestamp': self.backup_timestamp.isoformat()
        }
        
        # Add optional fields if they exist
        if self.description:
            item['description'] = self.description
        if self.principal_id:
            item['principal_id'] = self.principal_id
            
        return item
    
    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'Group':
        """
        Create Group instance from DynamoDB item.
        
        Args:
            item: DynamoDB item dictionary
            
        Returns:
            Group: Group instance with populated fields
        """
        backup_timestamp = datetime.fromisoformat(item['backup_timestamp']) if 'backup_timestamp' in item else datetime.now()
        
        return cls(
            group_name=item['group_name'],
            arn=item['arn'],
            description=item.get('description'),
            principal_id=item.get('principal_id'),
            members=item.get('members', []),
            backup_timestamp=backup_timestamp
        )


@dataclass
class UserGroupMembership:
    """QuickSight user-group membership relationship data model."""
    
    membership_id: str
    user_name: str
    group_name: str
    user_arn: str
    group_arn: str
    backup_timestamp: datetime = field(default_factory=datetime.now)
    
    @classmethod
    def create(cls, user: User, group: Group) -> 'UserGroupMembership':
        """
        Create UserGroupMembership instance from User and Group objects.
        
        Args:
            user: User object
            group: Group object
            
        Returns:
            UserGroupMembership: Membership instance
        """
        membership_id = f"{user.user_name}#{group.group_name}"
        return cls(
            membership_id=membership_id,
            user_name=user.user_name,
            group_name=group.group_name,
            user_arn=user.arn,
            group_arn=group.arn,
            backup_timestamp=datetime.now()
        )
    
    def to_dynamodb_item(self) -> Dict[str, Any]:
        """
        Convert UserGroupMembership instance to DynamoDB item format.
        
        Returns:
            Dict[str, Any]: DynamoDB item dictionary
        """
        return {
            'membership_id': self.membership_id,
            'user_name': self.user_name,
            'group_name': self.group_name,
            'user_arn': self.user_arn,
            'group_arn': self.group_arn,
            'backup_timestamp': self.backup_timestamp.isoformat()
        }
    
    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'UserGroupMembership':
        """
        Create UserGroupMembership instance from DynamoDB item.
        
        Args:
            item: DynamoDB item dictionary
            
        Returns:
            UserGroupMembership: Membership instance
        """
        backup_timestamp = datetime.fromisoformat(item['backup_timestamp']) if 'backup_timestamp' in item else datetime.now()
        
        return cls(
            membership_id=item['membership_id'],
            user_name=item['user_name'],
            group_name=item['group_name'],
            user_arn=item['user_arn'],
            group_arn=item['group_arn'],
            backup_timestamp=backup_timestamp
        )


def transform_users_from_api_response(api_users: List[Dict[str, Any]]) -> List[User]:
    """
    Transform list of QuickSight API user responses to User objects.
    
    Args:
        api_users: List of user dictionaries from QuickSight API
        
    Returns:
        List[User]: List of User objects
    """
    return [User.from_quicksight_api(user_data) for user_data in api_users]


def transform_groups_from_api_response(api_groups: List[Dict[str, Any]], 
                                     group_members: Optional[Dict[str, List[str]]] = None) -> List[Group]:
    """
    Transform list of QuickSight API group responses to Group objects.
    
    Args:
        api_groups: List of group dictionaries from QuickSight API
        group_members: Optional dictionary mapping group names to member lists
        
    Returns:
        List[Group]: List of Group objects
    """
    groups = []
    for group_data in api_groups:
        group_name = group_data.get('GroupName', '')
        members = group_members.get(group_name, []) if group_members else []
        groups.append(Group.from_quicksight_api(group_data, members))
    
    return groups


def users_to_dynamodb_items(users: List[User]) -> List[Dict[str, Any]]:
    """
    Convert list of User objects to DynamoDB items format.
    
    Args:
        users: List of User objects
        
    Returns:
        List[Dict[str, Any]]: List of DynamoDB item dictionaries
    """
    return [user.to_dynamodb_item() for user in users]


def groups_to_dynamodb_items(groups: List[Group]) -> List[Dict[str, Any]]:
    """
    Convert list of Group objects to DynamoDB items format.
    
    Args:
        groups: List of Group objects
        
    Returns:
        List[Dict[str, Any]]: List of DynamoDB item dictionaries
    """
    return [group.to_dynamodb_item() for group in groups]


def create_user_group_memberships(users: List[User], groups: List[Group]) -> List[UserGroupMembership]:
    """
    Create UserGroupMembership objects from users and groups.
    
    Args:
        users: List of User objects
        groups: List of Group objects
        
    Returns:
        List[UserGroupMembership]: List of membership relationships
    """
    memberships = []
    
    # Create a lookup dictionary for users by username
    user_lookup = {user.user_name: user for user in users}
    
    # Iterate through groups and their members
    for group in groups:
        for member_name in group.members:
            # Find the corresponding user
            user = user_lookup.get(member_name)
            if user:
                membership = UserGroupMembership.create(user, group)
                memberships.append(membership)
    
    return memberships


def user_group_memberships_to_dynamodb_items(memberships: List[UserGroupMembership]) -> List[Dict[str, Any]]:
    """
    Convert list of UserGroupMembership objects to DynamoDB items format.
    
    Args:
        memberships: List of UserGroupMembership objects
        
    Returns:
        List[Dict[str, Any]]: List of DynamoDB item dictionaries
    """
    return [membership.to_dynamodb_item() for membership in memberships]