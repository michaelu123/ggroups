# !/usr/bin/env python

# Python Quickstart
# https://developers.google.com/admin-sdk/directory/v1/quickstart/python
# Admin Directory API
# https://developers.google.com/resources/api-libraries/documentation/admin/directory_v1/python/latest/index.html

# https://stackoverflow.com/questions/62166670/getting-a-403-trying-to-list-directory-users

# manage custom user fields
# https://developers.google.com/admin-sdk/directory/v1/guides/manage-schemas

import argparse
import http.client
import os.path
import pprint
import json
import sys

import google.auth.transport.requests
from google.oauth2.credentials import Credentials
import google_auth_oauthlib.flow
import googleapiclient.discovery

parser = argparse.ArgumentParser()
parser.add_argument("-a", "--all", help="list members of all customer groups", action='store_true')
parser.add_argument("-d", "--domain", help="Domain name to display resources for")
parser.add_argument("--client-id",
                    help="Google Client ID for authorization. May also be specified with CLIENT_ID environment variable")
parser.add_argument("--client-secret",
                    help="Google Client Secret for authorization. May also be specified with CLIENT_SECRET environment variable")
args = parser.parse_args()

doIt = False

# The scope URL
SCOPES = ['https://www.googleapis.com/auth/admin.directory.group',
          'https://www.googleapis.com/auth/admin.directory.user',
          'https://www.googleapis.com/auth/admin.directory.customer',
          'https://www.googleapis.com/auth/admin.directory.rolemanagement',
          'https://www.googleapis.com/auth/admin.directory.userschema']

hdrs = {"Content-Type": "application/json", "Accept": "application/json, text/plain, */*",
        "Authorization": "Bearer undefined"}


class GGSync:
    def __init__(self):
        self.loginADB()
        try:
            with open("aktdb.json", "r") as fp:
                self.aktdb = json.load(fp)
                self.dbMembers = self.aktdb["emailToMember"]
                self.dbTeams = self.aktdb["teamName2Team"]
        except Exception as _e:
            dbMemberList = self.getDBMembers()
            dbTeamList = self.getDBTeams()
            # dbMem = getDBMember(token, dbMembers[0]["id"])
            self.aktdb = {"emailToMember": {}, "teamName2Team": {}}
            self.sortDB(dbMemberList, dbTeamList)

        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(google.auth.transport.requests.Request())
            else:
                flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        self.service = googleapiclient.discovery.build('admin', 'directory_v1', credentials=creds)

        try:
            with open("ggdb.json", "r") as fp:
                self.ggdb = json.load(fp)
                self.ggUsers = self.ggdb["email2User"]
                self.ggGroups = self.ggdb["groupName2Group"]
        except Exception as _e:
            print("get users list from GG")
            self.ggUsers = self.getGGUsers()
            print("get groups list from GG")
            self.ggGroups = self.getGGGroups()
            self.sortGG()

        self.karteiLeichenGrp = self.getGGGroup("noresponse@adfc-muenchen.de")

        # grp = ggGroups["MUTest"]
        # grp["members"] = getGGGroupMemberNames(service, grp["id"])
        # with open("ggdb.json", "w") as fp:
        #     json.dump(ggdb, fp, indent=2)

        """
        Nicht in Groups und Users:
        uhag@gmx.net Auch kein Member
        ellen.kramschuster@adfc-muenchen.de
        ag-verkehr_aktive@groups.adfc-muenchen.de
        """

        with open("mapping.json", "r") as fp:
            self.mapGrpA2G = json.load(fp)
            self.mapGrpG2A = {self.mapGrpA2G[k]: k for k in self.mapGrpA2G.keys()}
        with open("ignore_groups.json", "r") as fp:
            self.ignoreGroups = json.load(fp)

    def loginADB(self):
        with open("aktdb.creds") as fp:
            body = fp.read()
        hc = http.client.HTTPSConnection("aktivendb.adfc-muenchen.de")
        hc.request(method="POST", url="/auth/login", headers=hdrs, body=body)
        resp = hc.getresponse()
        # print("msg", resp.msg, resp.status, resp.reason)
        res = json.loads(resp.read())
        hc.close()
        self.token = res["token"]
        print("token", self.token)

    def getDBMembers(self):
        hc = http.client.HTTPSConnection("aktivendb.adfc-muenchen.de")
        hc.request(method="GET", url="/api/members?token=" + self.token, headers=hdrs)
        resp = hc.getresponse()
        # print("msg", resp.msg, resp.status, resp.reason)
        res = json.loads(resp.read())
        hc.close()
        return res

    def getDBMember(self, id):
        hc = http.client.HTTPSConnection("aktivendb.adfc-muenchen.de")
        hc.request(method="GET", url="/api/member/" + str(id) + "?token=" + self.token, headers=hdrs)
        resp = hc.getresponse()
        # print("msg", resp.msg, resp.status, resp.reason)
        res = json.loads(resp.read())
        hc.close()
        return res

    def getDBTeams(self):
        hc = http.client.HTTPSConnection("aktivendb.adfc-muenchen.de")
        hc.request(method="GET", url="/api/project-teams?token=" + self.token, headers=hdrs)
        resp = hc.getresponse()
        # print("msg", resp.msg, resp.status, resp.reason)
        res = json.loads(resp.read())
        hc.close()
        return res

    def getDBTeamMembers(self, id):
        hc = http.client.HTTPSConnection("aktivendb.adfc-muenchen.de")
        hc.request(method="GET", url="/api/project-team/" + str(id) + "?token=" + self.token, headers=hdrs)
        resp = hc.getresponse()
        # print("msg", resp.msg, resp.status, resp.reason)
        res = json.loads(resp.read())
        hc.close()
        return res

    def setDBEmail(self, id, email):
        hc = http.client.HTTPSConnection("aktivendb.adfc-muenchen.de")
        body = json.dumps({"email": email})
        hc.request(method="PUT", url="/api/project-team/" + str(id) + "?token=" + self.token, headers=hdrs, body=body)
        resp = hc.getresponse()
        # print("msg", resp.msg, resp.status, resp.reason)
        res = json.loads(resp.read())
        hc.close()
        return res

    def sortDB(self, dbMemberList, dbTeamList):
        for dbm in dbMemberList:
            for emKind in ["email_adfc", "email_private"]:
                if dbm[emKind] is not None and dbm[emKind] == "undef@undef.de":
                    dbm[emKind] = ""
        emailToMember = self.aktdb["emailToMember"]
        for emKind in ["email_adfc", "email_private"]:
            emailToMember[emKind] = {}
            for dbm in dbMemberList:
                email = dbm.get(emKind)
                if email is not None and email != "":
                    if emKind == "email_adfc":
                        if not email.endswith("@adfc-muenchen.de"):
                            print("ADFC_Email-Adresse falsch", email)
                    else:
                        if email.endswith("@adfc-muenchen.de"):
                            print("Private_Email-Adresse falsch", email)
                        em_adfc = dbm.get("email_adfc")
                        if em_adfc is not None and em_adfc != "":
                            continue
                    entries = emailToMember[emKind].get(email)
                    if entries is not None:
                        print(emKind, "mehrfach", email)
                    else:
                        entries = []
                        emailToMember[emKind][email] = entries
                    entries.append(dbm)
        teamName2Team = self.aktdb["teamName2Team"]
        for dbt in dbTeamList:
            teamName = dbt["name"]
            id = dbt["id"]
            teamName2Team[teamName] = dbt
            print("Team", teamName, id)
            dbt["detail"] = self.getDBTeamMembers(id)
            memberList = dbt["detail"]["members"]
            for m in memberList:
                for emKind in ["email_adfc", "email_private"]:
                    if m[emKind] is not None and m[emKind] == "undef@undef.de":
                        m[emKind] = ""

        self.dbMembers = self.aktdb["emailToMember"]
        self.dbTeams = self.aktdb["teamName2Team"]

        with open("aktdb.json", "w") as fp:
            json.dump(self.aktdb, fp, indent=2)

    def getGGUsers(self):
        page = None
        userList = []
        while True:
            requ = self.service.users().list(pageToken=page, domain="adfc-muenchen.de")
            respu = requ.execute()
            userList.extend(respu.get("users"))
            page = respu.get("nextPageToken")
            if page is None or page == "":
                break
        users = {u["primaryEmail"]: u for u in userList}
        return users

    def getGGGroups(self):
        # no privilege:
        # reqd = service.domains().list(customer="C01d138zo")
        # respd = reqd.execute()
        # domains = respd.get("domains")
        # domainNames = [d.domainName for d in domains]
        domainNames = ["adfc-muenchen.de", "groups.adfc-muenchen.de", "lists.adfc-muenchen.de"]
        page = None
        groupList = []
        for dn in domainNames:
            while True:
                reqg = self.service.groups().list(pageToken=page, domain=dn)
                respg = reqg.execute()
                groupList.extend(respg.get("groups"))
                page = respg.get("nextPageToken")
                if page is None or page == "":
                    break
        groups = {g["name"]: g for g in groupList}
        return groups

    def getGGMember(self, grpId, email):
        reqm = self.service.members().get(groupKey=grpId, memberKey=email)  # , projection="full"
        respm = reqm.execute()
        return respm

    def chgGGMemberRole(self, group, email, role):
        body = {"role": role}
        requ = self.service.members().update(groupKey=group["id"], memberKey=email, body=body)
        respu = requ.execute()
        return respu

    def getGGGroup(self, grpId):
        reqg = self.service.groups().get(groupKey=grpId)
        respg = reqg.execute()
        return respg

    def getGGGroupMemberNames(self, id):
        page = None
        memberList = []
        while True:
            reqg = self.service.members().list(groupKey=id, pageToken=page)  # , projection="full"
            respg = reqg.execute()
            ms = respg.get("members")
            if ms is not None:
                memberList.extend(ms)
            page = respg.get("nextPageToken")
            if page is None or page == "":
                break
        members = [{"email": m["email"].lower(), "role": m["role"]} for m in memberList if m["type"] == "USER"]
        return members

    def addMemberToGroup(self, group, email, role):
        body = {
            "kind": "admin#directory#member",
            "delivery_settings": "ALL_MAIL",
            "email": email,
            "role": role,  # MEMBER or MANAGER
            "type": "USER",
            "status": "ACTIVE",
        }
        try:
            reqm = self.service.members().insert(groupKey=group["id"], body=body)
            respm = reqm.execute()
            return respm
        except Exception as e:
            print("Error: cannot add member", email, "to group", group["name"], ":", e)
            return None

    def delMemberFromGroup(self, group, email):
        # body = {"password": "wahrscheinlich_Inaktives_Mitglied"}
        # try:
        #     requ = self.service.users().update(userKey=email, body=body)
        #     requ.execute()
        # except Exception as e:
        #     print("Error: cannot change password of", email, ":", e)

        self.addMemberToGroup(self.karteiLeichenGrp, email, "MEMBER")
        try:
            reqd = self.service.members().delete(groupKey=group["id"], memberKey=email)
            reqd.execute()
        except Exception as e:
            print("Error: cannot delete member", email, "from group", group["name"], ":", e)

    def addEmailToUser(self, user, privEmail):
        body = {
            "emails": [
                {
                    "address": privEmail,
                    "type": "other"
                }
            ]
        }
        try:
            requ = self.service.users().update(userKey=user["id"], body=body)
            respu = requ.execute()
            return respu
        except Exception as e:
            print("Error: cannot add email", privEmail, "to user", user["primaryEmail"], ":", e)
            return None

    def sortGG(self):
        self.ggdb = {}
        self.ggdb["email2User"] = self.ggUsers
        self.ggdb["groupName2Group"] = self.ggGroups

        aktTeamNames = list(self.dbTeams.keys())
        for g in sorted(self.ggGroups.values(), key=lambda g: g["name"]):
            g["members"] = []
            grpName = g["name"]
            if grpName in self.ignoreGroups:
                continue
            teamName = grpName
            if teamName.endswith("Leitung"):
                teamName = teamName.replace(" Leitung", "")
            if teamName.endswith("SprecherInnen"):
                teamName = teamName.replace(" SprecherInnen", "")
            mapped = self.mapGrpG2A.get(teamName)
            if mapped is not None:
                teamName = mapped
            if teamName in self.ignoreGroups:
                continue
            if teamName not in aktTeamNames:
                continue
            print("get groupmembers from", grpName)
            # g["members"] = self.getGGGroupMemberNames(g["id"])
        with open("ggdb.json", "w") as fp:
            json.dump(self.ggdb, fp, indent=2)

    def createGroup(self, grpName, leitung=False):
        grplName = grpName.lower()
        # if "test" in grplName or "leitung" in grplName or "fundraising" in grplName or "radfahrschule" in grplName:
        grpEmail = grplName.replace(" ", "-") \
            .replace("ortsgruppe", "og") \
            .replace("ä", "ae") \
            .replace("ö", "oe") \
            .replace("ü", "ue")
        if leitung:
            if "ortsgruppe" in grplName:
                grpEmail += "_sprecherinnen"
                desc = "SprecherInnen der " + grpName
                grpName += " SprecherInnen"
            else:
                grpEmail += "_leitung"
                desc = "Leitung der " + grpName
                grpName += " Leitung"
        else:
            grpEmail += "_aktive"
            desc = "Aktive der " + grpName
        grpEmail += "@groups.adfc-muenchen.de"
        grp = {
            "kind": "admin#directory#group",
            "email": grpEmail,
            "name": grpName,
            "description": desc,
        }
        print("Action: create group", grpName)
        pprint.pprint(grp)
        if doIt:
            try:
                req = self.service.groups().insert(body=grp)
                res = req.execute()
                return grpName, res
            except Exception as e:
                print("Cannot insert group", grpName, ":", e)
        return None, None

    def createMissingGroups(self):
        # compare AG names
        aktTeamNames = list(self.dbTeams.keys())
        ggGrpNames = list(self.ggGroups.keys())
        aktTeamNames.sort()
        ggGrpNames.sort()
        # print("a", aktTeamNames)
        # print("g", ggGrpNames)
        addedGroup = False
        for atn in aktTeamNames:
            if atn in self.ignoreGroups:
                continue
            grpName = atn
            mapped = self.mapGrpA2G.get(grpName)
            if mapped is not None:
                grpName = mapped
            if grpName in self.ignoreGroups:
                continue
            if grpName not in ggGrpNames:
                print("Action: create group", grpName)
                if doIt:
                    name, res = self.createGroup(grpName, leitung=False)
                    if res is not None:
                        self.ggGroups[name] = res
                        addedGroup = True
            leitung = " SprecherInnen" if "Ortsgruppe" in grpName else " Leitung"
            if (grpName + leitung) in self.ignoreGroups:
                continue
            if (grpName + leitung) not in ggGrpNames:
                print("Action: create group", grpName + leitung)
                name, res = self.createGroup(grpName, leitung=True)
                if res is not None:
                    addedGroup = True
        if addedGroup:
            print("Please delete ggdb.json and start again")
            sys.exit(0)

    def printUnmatchedDBGroups(self):
        aktTeamNames = list(self.dbTeams.keys())
        aktTeamNames.sort()
        ggGrpNames = list(self.ggGroups.keys())
        ggGrpNames.sort()
        print("\n\nAG/OG Missing in AktivenDB")
        for grpName in ggGrpNames:
            if grpName.endswith("Leitung") or "Sprecher" in grpName:
                continue
            mapped = self.mapGrpG2A.get(grpName)
            if mapped is not None:
                grpName = mapped
            if grpName not in aktTeamNames:
                print(grpName)

    def printUnmatchedDBUsers(self):
        aktMemberNames = list(self.dbMembers["email_adfc"].keys())
        aktMemberNames = [e.lower() for e in aktMemberNames]
        aktMemberNames.sort()
        ggUserNames = list(self.ggUsers.keys())
        ggUserNames.sort()
        print("\n\nMember Missing in AktivenDB")
        for gun in ggUserNames:
            if gun not in aktMemberNames:
                print(gun)

    def addTeamEmailAddressesToAktb(self):
        for team in sorted(self.dbTeams.values(), key=lambda t: t["name"]):
            name = team["name"]
            mapped = self.mapGrpA2G.get(name)
            if mapped is not None:
                name = mapped
            grp = self.ggGroups.get(name)
            if grp is None:
                continue
            if team["email"] != grp["email"]:
                # print("a:", team["email"], "g", grp["email"])
                print("Action(in AktivenDB): set email of team", name, "to", grp["email"])
                if doIt:
                    self.setDBEmail(team["id"], grp["email"])

    def memberInGroup(self, grpName, email):
        grp = self.ggGroups.get(grpName)
        if grp is None:
            return None
        members = grp["members"]
        emails = [m["email"] for m in members]
        return email in emails

    def addToGG(self):
        addedEmails = {}
        for team in sorted(self.dbTeams.values(), key=lambda t: t["name"]):
            teamName = team["name"]
            if teamName in self.ignoreGroups:
                continue
            grpName = teamName
            mapped = self.mapGrpA2G.get(teamName)
            if mapped is not None:
                grpName = mapped
            grp = self.ggGroups.get(grpName)
            if grp is None:
                print("cannot find group", grpName, "for team", teamName)
                continue
            for member in team["detail"]["members"]:
                aktRole = member["project_team_member"]["member_role_id"]
                ggRole = "MANAGER" if aktRole == '1' else "MEMBER"
                adfcEmail = member["email_adfc"].lower()
                user = None if adfcEmail == "" else self.ggUsers.get(adfcEmail)
                if adfcEmail != "" and user is None:
                    print("Missing user ", adfcEmail, "of team", teamName)
                    member["missingUser"] = True
                    # Create User?
                    continue
                privEmail = member["email_private"].lower()

                if privEmail != "" and user is not None:
                    emails = [email["address"] for email in user["emails"]]
                    if privEmail not in emails and addedEmails.get(privEmail + adfcEmail) is None:
                        member["missingEmail"] = True
                        user["missingEmail"] = True
                        print("Action: add email", privEmail, "to user", adfcEmail)
                        if doIt:
                            self.addEmailToUser(user, privEmail)
                        addedEmails[privEmail + adfcEmail] = True

                foundEmail = None
                if adfcEmail != "" and self.memberInGroup(grpName, adfcEmail):
                    foundEmail = adfcEmail
                elif privEmail != "" and self.memberInGroup(grpName, privEmail):
                    foundEmail = privEmail
                email = adfcEmail or privEmail
                if email == "":
                    print("no email for ", member["name"])
                    continue
                if adfcEmail != "" and foundEmail is not None and foundEmail != adfcEmail:
                    print("Action: add member", adfcEmail, "in addition of", foundEmail, "to", grpName)
                    if doIt:
                        self.addMemberToGroup(grp, adfcEmail, ggRole)
                elif foundEmail is None:
                    print("Action: add member", email, "to", grpName)
                    if doIt:
                        self.addMemberToGroup(grp, email, ggRole)
                else:
                    gmembers = grp["members"]
                    gmember = next((gm for gm in gmembers if gm["email"] == foundEmail), None)
                    if gmember is not None and ggRole != gmember["role"]:
                        print("Action: change role of ", foundEmail, "in group", grpName, "from", gmember["role"], "to",
                              ggRole)
                        if doIt:
                            self.chgGGMemberRole(grp, email, ggRole)

                lgrp = self.ggGroups.get(grpName + " Leitung")
                if lgrp is None:
                    lgrp = self.ggGroups.get(grpName + " SprecherInnen")
                if lgrp is None:
                    print("No leader group for", email, "of group", grpName)
                    continue
                lgrpName = lgrp["name"]
                foundEmail = None
                if adfcEmail != "" and self.memberInGroup(lgrpName, adfcEmail):
                    foundEmail = adfcEmail
                elif privEmail != "" and self.memberInGroup(lgrpName, privEmail):
                    foundEmail = privEmail
                if ggRole == 'MANAGER':  # Vorsitz, add to group Leitung/Sprecherinnen
                    if foundEmail is None:
                        print("Action: add member", email, "to", lgrpName)
                        if doIt:
                            self.addMemberToGroup(lgrp, email, "MEMBER")  # TODO MANAGER?
                elif gmember["role"] == "MANAGER":
                    if foundEmail is not None:
                        print("Action: remove member", foundEmail, "from", lgrpName)
                        if doIt:
                            self.delMemberFromGroup(lgrp, foundEmail)

    def removeFromGG(self):
        aktTeamNames = list(self.dbTeams.keys())
        aktTeamNames.sort()
        missingAktdb = {}
        noEmail = {}
        for grp in sorted(self.ggGroups.values(), key=lambda g: g["name"]):
            grpName = grp["name"]
            if grpName in self.ignoreGroups:
                continue
            teamName = grpName
            leiterGrp = False
            if teamName.endswith("Leitung"):
                teamName = teamName.replace(" Leitung", "")
                leiterGrp = True
            if teamName in self.ignoreGroups:
                continue
            if teamName.endswith("SprecherInnen"):
                teamName = teamName.replace(" SprecherInnen", "")
                leiterGrp = True
            mapped = self.mapGrpG2A.get(teamName)
            if mapped is not None:
                teamName = mapped
            if teamName not in aktTeamNames:
                continue
            team = self.dbTeams[teamName]
            gmembers = grp["members"]
            gmemberEmails = [gm["email"] for gm in gmembers]  # XXX
            amembers = []
            for tmember in team["detail"]["members"]:
                adfcEmail = tmember["email_adfc"].lower()
                privEmail = tmember["email_private"].lower()
                email = adfcEmail or privEmail
                if email == "" and noEmail.get(tmember["name"]) is None:
                    print("no email", tmember["name"], "in group", grpName)
                    noEmail[tmember["name"]] = 1
                    continue
                if leiterGrp:
                    if tmember["project_team_member"]["member_role_id"] == '1':
                        if email != adfcEmail:
                            print("leiter", email, "of group", grpName, "is no user")
                            continue
                        amembers.append(email)
                else:
                    amembers.append(email)
            for gmemberEmail in gmemberEmails:
                if gmemberEmail not in amembers:
                    if "leitung@" in gmemberEmail:
                        continue
                    if self.dbMembers["email_adfc"].get(gmemberEmail) is None \
                            and self.dbMembers["email_private"].get(gmemberEmail) is None:
                        if missingAktdb.get(gmemberEmail) is None:
                            print("Missing", gmemberEmail, "in group", grpName, "from AktivenDB")
                            missingAktdb[gmemberEmail] = True

                    print("Action: delete", gmemberEmail, "from", grpName)
                    while True:
                        inp = "y"  # inp = input("Shall I? (y/n)")
                        if inp == 'y':
                            if doIt:
                                self.delMemberFromGroup(gmemberEmail, grp)
                            break
                        if inp == 'n':
                            break
                    pass
        # TODO when manager->member, remove from leitung

    def main(self):
        # self.createMissingGroups()
        # self.printUnmatchedDBGroups()
        # self.printUnmatchedDBUsers()
        # self.addTeamEmailAddressesToAktb()
        # self.addToGG()

        while True:
            inp = "y"  # inp = input("Remove members from groups? (y/n)")
            if inp == 'y':
                self.removeFromGG()
                break
            if inp == 'n':
                break
        pass


if __name__ == '__main__':
    ggsync = GGSync()
    ggsync.main()
